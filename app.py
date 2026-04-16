from flask import Flask, request, jsonify
import paramiko
import gspread
import gspread.exceptions
import json
import base64
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import os
import platform
import io
import re
import threading
import time
import uuid

app = Flask(__name__)

# SFTPアカウント情報のキャッシュ（Sheets APIのレート制限対策）
# TTLは環境変数 CREDENTIALS_CACHE_TTL で変更可（秒、デフォルト300）
_creds_cache: dict = {}
_creds_cache_at: float = 0.0
_creds_cache_lock = threading.Lock()
_CREDENTIALS_CACHE_TTL = max(0, int(os.getenv("CREDENTIALS_CACHE_TTL", "60")))

# ✅ Renderではcredentials.jsonではなく環境変数から
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDENTIALS_JSON:
    raise RuntimeError("❌ 環境変数 GOOGLE_CREDENTIALS_JSON が未設定")

creds_dict = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_JSON).decode("utf-8"))
creds = Credentials.from_service_account_info(creds_dict, scopes=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
])
print("✅ Google認証成功")

# 固定情報
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1_t8pThdb0kFyIyRfNtC-VLsGa6HopgGQoEOqKyisjME"
FOLDER_ID = os.getenv("FOLDER_ID") or "1ykCNsVXqi619OzXwLTqVJIm1WbqWcMgn"
SHEET_ACCOUNTS = "アカウント管理"
SHEET_RESERVATIONS = "アップロード予約"
SFTP_HOST = "upload.rakuten.ne.jp"
SFTP_PORT = 22
SFTP_UPLOAD_PATH = "/ritem/batch"

drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)
gspread_client = gspread.authorize(creds)

def normalize(text):
    if not isinstance(text, str):
        return ""
    return re.sub(r"[\u3000\u200b\s\r\n]", "", text.strip().lower())

def _fetch_all_credentials():
    """Sheets APIからアカウント情報を全件取得してキャッシュを更新する。"""
    global _creds_cache, _creds_cache_at
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_ACCOUNTS}!A1:C"
    ).execute()
    values = result.get("values", [])
    if not values or len(values) < 2:
        _creds_cache = {}
        _creds_cache_at = time.monotonic()
        return
    headers = values[0]
    rows = values[1:]
    idx_account = headers.index("アカウント名")
    idx_user = headers.index("FTP用ユーザー名")
    idx_pass = headers.index("FTP用パスワード")
    _creds_cache = {
        normalize(row[idx_account]): (row[idx_user].strip(), row[idx_pass].strip())
        for row in rows
        if len(row) > max(idx_account, idx_user, idx_pass)
    }
    _creds_cache_at = time.monotonic()


def get_sftp_credentials(account_name):
    global _creds_cache, _creds_cache_at
    normalized_input = normalize(account_name)
    with _creds_cache_lock:
        if time.monotonic() - _creds_cache_at > _CREDENTIALS_CACHE_TTL:
            try:
                _fetch_all_credentials()
            except Exception as e:
                print(f"❌ SFTP認証取得エラー: {e}")
                return None, None
        return _creds_cache.get(normalized_input, (None, None))

def update_sheet_status(filename, status, error_message=""):
    for attempt in range(4):
        try:
            sheet = gspread_client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
            data = sheet.get_all_values()
            headers = data[0]
            filename_col = headers.index("ファイル名")
            status_col = headers.index("ステータス")
            error_col = headers.index("エラーメッセージ") if "エラーメッセージ" in headers else len(headers)

            if "エラーメッセージ" not in headers:
                sheet.update_cell(1, error_col + 1, "エラーメッセージ")

            for i, row in enumerate(data[1:], start=2):
                if row[filename_col] == filename:
                    sheet.update_cell(i, status_col + 1, status)
                    sheet.update_cell(i, error_col + 1, error_message)
                    return
            return
        except gspread.exceptions.APIError as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 429 and attempt < 3:
                wait = 2 ** attempt
                print(f"⚠️ Sheets API 429 - {wait}秒後にリトライ ({attempt + 1}/3)")
                time.sleep(wait)
                continue
            print(f"❌ スプレッドシート更新エラー: {e}")
            return
        except Exception as e:
            print(f"❌ スプレッドシート更新エラー: {e}")
            return

def get_google_drive_file_path(filename):
    try:
        query = f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false"
        result = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except Exception as e:
        print(f"❌ Driveファイル検索エラー: {e}")
        return None

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "running"})

@app.route("/upload_sftp", methods=["POST"])
def upload_sftp():
    try:
        data = request.get_json()
        account = data.get("account")
        filename = data.get("filename")

        if not account or not filename:
            return jsonify({"status": "error", "message": "アカウントまたはファイル名が不足"}), 400

        username, password = get_sftp_credentials(account)
        if not username or not password:
            update_sheet_status(filename, "エラー", "FTPアカウント情報が見つかりません")
            return jsonify({"status": "error", "message": "FTPアカウント情報が見つかりません"}), 400

        file_id = get_google_drive_file_path(filename)
        if not file_id:
            update_sheet_status(filename, "エラー", "Google Drive にファイルが見つかりません")
            return jsonify({"status": "error", "message": "Google Drive にファイルが見つかりません"}), 404

        tmp_dir = "/tmp" if platform.system() != "Windows" else "./tmp"
        os.makedirs(tmp_dir, exist_ok=True)

        safe_name = os.path.basename(filename) or "upload.bin"
        run_id = uuid.uuid4().hex
        file_path = os.path.join(tmp_dir, f"{run_id}_{safe_name}")

        try:
            request_drive = drive_service.files().get_media(fileId=file_id)
            with open(file_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request_drive)
                done = False
                while not done:
                    status, done = downloader.next_chunk()

            transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put(file_path, f"{SFTP_UPLOAD_PATH}/{filename}")
            # put() 成功後のクリーンアップ。失敗してもアップロード結果に影響させない
            try:
                sftp.close()
            except Exception:
                pass
            try:
                transport.close()
            except Exception:
                pass
        finally:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    pass

        update_sheet_status(filename, "アップロード完了")
        return jsonify({"status": "success", "message": f"{filename} のアップロード成功"})
    except Exception as e:
        print(f"❌ `/upload_sftp` エラー: {e}")
        update_sheet_status(data.get("filename", "不明"), "エラー", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes"),
    )
