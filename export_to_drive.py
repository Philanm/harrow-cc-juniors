#!/usr/bin/env python3
"""
export_to_drive.py
------------------
Reads attendance data from Firebase Realtime Database for the most recent
Sunday session and uploads a CSV to a Google Drive folder.

Runs via GitHub Actions after each Sunday session (scheduled for 23:00 BST
= 22:00 UTC, giving coaches the full day to mark attendance).

Required environment variables (set as GitHub Secrets):
  FIREBASE_DATABASE_URL       — e.g. https://your-project.firebaseio.com
  GOOGLE_SERVICE_ACCOUNT_JSON — full JSON contents of the service account key
  GOOGLE_DRIVE_FOLDER_ID      — ID of the Drive folder to save CSVs into
"""

import csv
import io
import json
import os
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

# ========================= CONFIG =========================
# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = 'service-account-key.json'   # Put this in your repo or use GitHub Secrets + checkout

# Your Google Drive Folder ID (or Shared Drive ID)
GOOGLE_DRIVE_FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID')   # Better to use environment variable

# Firebase config (keep your existing Firebase setup)
# ... your existing Firebase code here ...

# =========================================================

def get_drive_service():
    """Create and return an authenticated Google Drive service using service account."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/drive']   # Full Drive scope - most reliable
        )
        
        service = build('drive', 'v3', credentials=credentials)
        print("    Google Drive service created successfully (Service Account)")
        return service
    except Exception as e:
        print(f"    ERROR creating Drive service: {e}")
        raise


def upload_to_drive(service, csv_content: str, filename: str, folder_id: str) -> str:
    """Upload CSV content to Google Drive using the official client library."""
    print(f"  Uploading: {filename} to folder {folder_id}")
    
    # Convert string to file-like object
    file_stream = io.BytesIO(csv_content.encode('utf-8'))
    
    media = MediaIoBaseUpload(
        file_stream,
        mimetype='text/csv',
        resumable=True
    )
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id],
        'mimeType': 'text/csv'
    }
    
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()
        
        print(f"    Upload successful! File ID: {file.get('id')}")
        print(f"    View link: {file.get('webViewLink')}")
        
        return file.get('webViewLink')
        
    except HttpError as error:
        print(f"    Google Drive API Error: {error}")
        try:
            error_details = json.loads(error.content.decode())
            print("    Error details:")
            print(json.dumps(error_details, indent=2))
        except:
            print(f"    Raw error: {error.content}")
        
        # Helpful guidance for common 403 issues
        if error.resp.status == 403:
            print("\n    === HOW TO FIX 403 FORBIDDEN WITH SERVICE ACCOUNT ===")
            print("    1. Share the target folder (or Shared Drive) with your service account email")
            print("       (e.g. your-project@your-project.iam.gserviceaccount.com) → give it 'Editor' access")
            print("    2. Strongly recommended: Use a **Shared Drive** instead of a normal My Drive folder")
            print("    3. Make sure the service account has the 'https://www.googleapis.com/auth/drive' scope")
        raise


def main():
    print("=============================================================")
    print("HTCC Juniors — Session Export to Google Drive")
    print(f"Run at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=============================================================")
    
    # Example: Export one session (adapt to your loop if you have multiple)
    session_date = "12 Apr 2026"
    session_name = "Session 1"
    firebase_key = "session_1_12_Apr_2026"
    filename = f"HTCC_Juniors_{session_date.replace(' ', '_')}_{session_name.replace(' ', '')}.csv"
    
    print(f"Exporting: {session_name} — {session_date}")
    print(f"Firebase key: {firebase_key}")
    
    # 1. Fetch from Firebase (keep your existing code here)
    print("1. Fetching attendance from Firebase...")
    # ... your existing Firebase fetching logic ...
    # For this example, we'll assume you build csv_content below
    
    # 2. Build CSV (replace with your actual CSV generation)
    print("2. Building CSV...")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Name", "Status", "Notes"])   # Add your real headers
    # writer.writerows(your_data)   # ← add your actual rows here
    csv_content = output.getvalue()
    
    print(f"   → Filename: {filename}")
    
    # 3. Upload
    print("3. Authenticating with Google Drive...")
    service = get_drive_service()
    
    print("4. Uploading to Google Drive...")
    try:
        file_url = upload_to_drive(service, csv_content, filename, GOOGLE_DRIVE_FOLDER_ID)
        print(f"   → Success! File uploaded: {file_url}")
    except Exception as e:
        print(f"   Upload failed: {e}")
        raise


if __name__ == "__main__":
    main()
