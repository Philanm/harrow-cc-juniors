#!/usr/bin/env python3
"""
export_to_drive.py
------------------
Reads attendance data from Firebase Realtime Database for the most recent
Sunday session and uploads a CSV to a Google Drive folder.

Required GitHub Secrets:
  FIREBASE_DATABASE_URL       — e.g. https://your-project.firebaseio.com
  GOOGLE_SERVICE_ACCOUNT_JSON — full JSON contents of the service account key
  GOOGLE_DRIVE_FOLDER_ID      — ID of the Drive folder to save CSVs into
"""

import csv
import io
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────────────────────────
FIREBASE_DATABASE_URL       = os.environ.get("FIREBASE_DATABASE_URL", "").rstrip("/")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_DRIVE_FOLDER_ID      = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

SESSIONS = [
    "12 Apr 2026", "19 Apr 2026", "26 Apr 2026", "03 May 2026", "10 May 2026",
    "17 May 2026", "24 May 2026", "31 May 2026", "07 Jun 2026", "14 Jun 2026",
    "21 Jun 2026", "28 Jun 2026", "05 Jul 2026", "12 Jul 2026"
]

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

# ── SESSION HELPERS ───────────────────────────────────────────────────────────

def session_date(session_str):
    d, m, y = session_str.split()
    return datetime(int(y), MONTHS[m], int(d), tzinfo=timezone.utc)


def find_most_recent_session():
    today = datetime.now(timezone.utc).date()
    past = [(i, s) for i, s in enumerate(SESSIONS) if session_date(s).date() <= today]
    return past[-1] if past else None


def make_session_key(idx, session_str):
    return f"session_{idx + 1}_{session_str.replace(' ', '_')}"


# ── FIREBASE ──────────────────────────────────────────────────────────────────

def fetch_firebase_session(session_key):
    url = f"{FIREBASE_DATABASE_URL}/attendance/{session_key}.json"
    print(f"  Fetching: {url}")
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            raw = resp.read().decode("utf-8").strip()
            if not raw or raw == "null":
                return {}
            return json.loads(raw)
    except Exception as e:
        print(f"  WARNING: Could not fetch Firebase data: {e}")
        return {}


# ── CSV BUILDER ───────────────────────────────────────────────────────────────

def build_csv(session_label, data):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Session", "Name", "Type", "Status",
        "School Year", "DOB", "Parent / Guardian",
        "Parent Tel", "Parent Email",
        "Emergency Contact", "Emergency Tel", "Medical Notes",
        "Last Updated"
    ])
    if not data:
        writer.writerow([session_label, "No attendance data recorded",
                         "", "", "", "", "", "", "", "", "", "", ""])
    else:
        for _, record in sorted(data.items(), key=lambda x: x[1].get("name", "").lower()):
            writer.writerow([
                session_label,
                record.get("name", ""),
                record.get("type", "registered"),
                record.get("status", "absent"),
                record.get("year", ""),
                record.get("dob", ""),
                record.get("parent", ""),
                record.get("parentTel", ""),
                record.get("parentEmail", ""),
                record.get("emergency", ""),
                record.get("emergencyTel", ""),
                record.get("medical", ""),
                record.get("updatedAt", ""),
            ])
    return output.getvalue()


# ── GOOGLE DRIVE ──────────────────────────────────────────────────────────────

def get_drive_service(service_account_json):
    """Build authenticated Google Drive service using the official client library."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        print("ERROR: google-api-python-client or google-auth not installed.")
        sys.exit(1)

    try:
        creds_dict = json.loads(service_account_json)
    except Exception as e:
        print(f"ERROR: Could not parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
        sys.exit(1)

    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_to_drive(csv_content, filename, folder_id, drive_service):
    """Upload or update a CSV file in Google Drive."""
    from googleapiclient.http import MediaIoBaseUpload

    media = MediaIoBaseUpload(
        io.BytesIO(csv_content.encode("utf-8")),
        mimetype="text/csv",
        resumable=False
    )

    # Check if file already exists so we update rather than duplicate
    results = drive_service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id, name)"
    ).execute()
    existing = results.get("files", [])

    if existing:
        file_id = existing[0]["id"]
        print(f"  Updating existing file: {filename} (id: {file_id})")
        result = drive_service.files().update(
            fileId=file_id,
            media_body=media,
            fields="id,webViewLink"
        ).execute()
    else:
        print(f"  Creating new file: {filename}")
        result = drive_service.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id,webViewLink"
        ).execute()

    return result.get("webViewLink", f"https://drive.google.com/drive/folders/{folder_id}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"HTCC Juniors — Session Export to Google Drive")
    print(f"Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # Validate secrets
    missing = [k for k, v in {
        "FIREBASE_DATABASE_URL":       FIREBASE_DATABASE_URL,
        "GOOGLE_SERVICE_ACCOUNT_JSON": GOOGLE_SERVICE_ACCOUNT_JSON,
        "GOOGLE_DRIVE_FOLDER_ID":      GOOGLE_DRIVE_FOLDER_ID,
    }.items() if not v]
    if missing:
        print(f"ERROR: Missing required secrets: {', '.join(missing)}")
        sys.exit(1)

    # Find session
    result = find_most_recent_session()
    if not result:
        print("No sessions found in the past. Nothing to export.")
        sys.exit(0)

    session_idx, session_str = result
    session_key   = make_session_key(session_idx, session_str)
    session_label = f"Session {session_idx + 1} — {session_str}"

    print(f"Exporting: {session_label}")
    print(f"Firebase key: {session_key}\n")

    # Fetch from Firebase
    print("1. Fetching attendance from Firebase...")
    data    = fetch_firebase_session(session_key)
    present = sum(1 for r in data.values() if r.get("status") == "present")
    absent  = sum(1 for r in data.values() if r.get("status") == "absent")
    walkins = sum(1 for r in data.values() if r.get("type") == "walk-in")
    print(f"   → {len(data)} records | {present} present | {absent} absent | {walkins} walk-ins")

    # Build CSV
    print("\n2. Building CSV...")
    csv_content = build_csv(session_label, data)
    filename    = f"HTCC_Juniors_{session_str.replace(' ', '_')}_Session{session_idx+1}.csv"
    print(f"   → Filename: {filename}")

    # Authenticate
    print("\n3. Authenticating with Google Drive...")
    drive_service = get_drive_service(GOOGLE_SERVICE_ACCOUNT_JSON)
    print("   → Authenticated ✓")

    # Upload
    print("\n4. Uploading to Google Drive...")
    file_url = upload_to_drive(csv_content, filename, GOOGLE_DRIVE_FOLDER_ID, drive_service)
    print(f"   → Uploaded ✓")
    print(f"   → {file_url}")

    print(f"\n✅ Done! '{filename}' saved to Google Drive.\n")


if __name__ == "__main__":
    main()
