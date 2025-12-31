import requests
import zipfile
import io
import pandas as pd
import datetime
import sys

# ---------------- CONFIG ----------------
API_URL = "https://www.nseindia.com/api/reports"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports"
}

ARCHIVES_PAYLOAD = [{
    "name": "CM-UDiFF Common Bhavcopy Final (zip)",
    "type": "daily-reports",
    "category": "capital-market",
    "section": "equities"
}]

# ---------------- DATE (Yesterday) ----------------
yesterday = datetime.date.today() - datetime.timedelta(days=1)
date_str = yesterday.strftime("%d-%b-%Y")
print(f"📅 Requesting NSE Bhavcopy for: {date_str}")

# ---------------- SESSION ----------------
session = requests.Session()
session.headers.update(HEADERS)

# Mandatory warm-up
session.get("https://www.nseindia.com", timeout=10)

params = {
    "archives": str(ARCHIVES_PAYLOAD).replace("'", '"'),
    "date": date_str,
    "type": "equities",
    "mode": "single"
}

resp = session.get(API_URL, params=params, timeout=20)

content_type = resp.headers.get("Content-Type", "").lower()
print("📦 NSE Response Content-Type:", content_type)

# ---------------- CASE 1: NSE RETURNS ZIP DIRECTLY ----------------
if "zip" in content_type:
    print("✅ NSE returned ZIP directly")

    z = zipfile.ZipFile(io.BytesIO(resp.content))
    csv_name = z.namelist()[0]
    df = pd.read_csv(z.open(csv_name))

# ---------------- CASE 2: NSE RETURNS JSON ----------------
elif "json" in content_type:
    print("ℹ NSE returned JSON metadata")

    data = resp.json()
    if not data or "filePath" not in data[0]:
        print("❌ JSON response but no filePath")
        sys.exit(1)

    zip_url = "https://archives.nseindia.com" + data[0]["filePath"]
    print("⬇ Downloading:", zip_url)

    zip_resp = session.get(zip_url, timeout=20)
    zip_resp.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(zip_resp.content))
    csv_name = z.namelist()[0]
    df = pd.read_csv(z.open(csv_name))

# ---------------- CASE 3: NSE BLOCKED / HTML ----------------
else:
    print("❌ NSE bhavcopy not released yet")
    sys.exit(1)

# ---- Normalize column names (UDiFF compatible) ----
df.columns = [c.strip() for c in df.columns]

COLUMN_MAP = {
    "ISIN": "ISIN",
    "TradDt": "TradDt",
    "TckrSymb": "TckrSymb",
    "ClsPric": "ClsPric"
}

missing = [c for c in COLUMN_MAP if c not in df.columns]
if missing:
    print("❌ Missing required columns:", missing)
    print("Available columns:", list(df.columns))
    sys.exit(1)

final_df = df[list(COLUMN_MAP.keys())]
final_df = final_df.rename(columns=COLUMN_MAP)


final_df.to_csv("NSE_Bhavcopy.csv", index=False)
print("✅ NSE Bhavcopy saved successfully")

import gspread
from google.oauth2.service_account import Credentials
import json
import os

# ---- Google Sheets Auth ----
creds_dict = json.loads(os.environ["GSHEET_CREDS"])
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(creds)

sheet = gc.open(os.environ["SHEET_NAME"])
nse_ws = sheet.worksheet("NSE")

# ---- Clear old data ----
nse_ws.clear()

# ---- Upload new data ----
nse_ws.update(
    [final_df.columns.tolist()] + final_df.values.tolist()
)

print("✅ NSE data uploaded to Google Sheets")

