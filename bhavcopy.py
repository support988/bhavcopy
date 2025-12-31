import requests
import zipfile
import io
import pandas as pd
import datetime
import sys
import os
import json
import gspread
from google.oauth2.service_account import Credentials

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
trade_date = datetime.date.today() - datetime.timedelta(days=1)
date_str = trade_date.strftime("%d-%b-%Y")
print(f"📅 Requesting NSE Bhavcopy for: {date_str}")

# ---------------- SESSION ----------------
session = requests.Session()
session.headers.update(HEADERS)

session.get("https://www.nseindia.com", timeout=10)

params = {
    "archives": json.dumps(ARCHIVES_PAYLOAD),
    "date": date_str,
    "type": "equities",
    "mode": "single"
}

resp = session.get(API_URL, params=params, timeout=20)
content_type = resp.headers.get("Content-Type", "").lower()

print("📦 NSE Response Content-Type:", content_type)

# ---------------- DOWNLOAD ----------------
if "zip" in content_type:
    print("✅ NSE returned ZIP directly")
    z = zipfile.ZipFile(io.BytesIO(resp.content))

elif "json" in content_type:
    print("ℹ NSE returned JSON metadata")
    data = resp.json()
    if not data or "filePath" not in data[0]:
        print("❌ JSON received but no filePath")
        sys.exit(1)

    zip_url = "https://archives.nseindia.com" + data[0]["filePath"]
    z = zipfile.ZipFile(io.BytesIO(session.get(zip_url).content))

else:
    print("❌ NSE bhavcopy not available yet")
    sys.exit(1)

csv_name = z.namelist()[0]
df = pd.read_csv(z.open(csv_name))
df.columns = [c.strip() for c in df.columns]

# ---------------- COLUMN NORMALIZATION ----------------
COLUMN_MAP = {
    "ISIN": "ISIN",
    "TradDt": "Trade_Date",
    "TckrSymb": "Symbol",
    "ClsPric": "Close_Price"
}

missing = [c for c in COLUMN_MAP if c not in df.columns]
if missing:
    print("❌ Missing columns:", missing)
    print("Available:", list(df.columns))
    sys.exit(1)

final_df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

final_df.to_csv("NSE_Bhavcopy.csv", index=False)
print("✅ NSE Bhavcopy saved locally")

# ---------------- GOOGLE SHEETS UPLOAD ----------------
creds_dict = json.loads(os.environ["GSHEET_CREDS"])
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(creds)

sheet = gc.open_by_key(os.environ["SPREADSHEET_ID"])
worksheet = sheet.worksheet("NSE")

worksheet.clear()
worksheet.update([final_df.columns.tolist()] + final_df.values.tolist())

print(f"✅ Uploaded {len(final_df)} rows to Google Sheets")
