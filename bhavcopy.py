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

# =========================
# COMMON CONFIG
# =========================
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports"
}

trade_date = datetime.date.today() - datetime.timedelta(days=1)
date_str_nse = trade_date.strftime("%d-%b-%Y")
date_str_bse = trade_date.strftime("%Y%m%d")

print(f"📅 Trade Date (Yesterday): {trade_date}")

# =========================
# GOOGLE SHEETS AUTH
# =========================
creds_dict = json.loads(os.environ["GSHEET_CREDS"])
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(creds)

sheet = gc.open_by_key(os.environ["SPREADSHEET_ID"])

# =========================
# ========== NSE ==========
# =========================
print("\n🚀 Fetching NSE Bhavcopy")

API_URL = "https://www.nseindia.com/api/reports"

ARCHIVES_PAYLOAD = [{
    "name": "CM-UDiFF Common Bhavcopy Final (zip)",
    "type": "daily-reports",
    "category": "capital-market",
    "section": "equities"
}]

session = requests.Session()
session.headers.update(HEADERS)

# Warm-up (MANDATORY for NSE)
session.get("https://www.nseindia.com", timeout=10)

params = {
    "archives": json.dumps(ARCHIVES_PAYLOAD),
    "date": date_str_nse,
    "type": "equities",
    "mode": "single"
}

resp = session.get(API_URL, params=params, timeout=20)
content_type = resp.headers.get("Content-Type", "").lower()
print("📦 NSE Response Content-Type:", content_type)

if "zip" in content_type:
    z = zipfile.ZipFile(io.BytesIO(resp.content))

elif "json" in content_type:
    data = resp.json()
    if not data or "filePath" not in data[0]:
        raise Exception("❌ NSE JSON received but no filePath")

    zip_url = "https://archives.nseindia.com" + data[0]["filePath"]
    z = zipfile.ZipFile(io.BytesIO(session.get(zip_url).content))

else:
    raise Exception("❌ NSE Bhavcopy not available")

csv_name = z.namelist()[0]
df_nse = pd.read_csv(z.open(csv_name))
df_nse.columns = [c.strip() for c in df_nse.columns]

COLUMN_MAP_NSE = {
    "ISIN": "ISIN",
    "TradDt": "Trade_Date",
    "TckrSymb": "Symbol",
    "ClsPric": "Close_Price"
}

df_nse_final = df_nse[list(COLUMN_MAP_NSE.keys())].rename(columns=COLUMN_MAP_NSE)

ws_nse = sheet.worksheet("NSE")
ws_nse.clear()
ws_nse.update([df_nse_final.columns.tolist()] + df_nse_final.values.tolist())

print(f"✅ NSE uploaded: {len(df_nse_final)} rows")

# =========================
# ========== BSE ==========
# =========================
print("\n🚀 Fetching BSE Bhavcopy")

bse_url = (
    "https://www.bseindia.com/download/BhavCopy/Equity/"
    f"BhavCopy_BSE_CM_0_0_0_{date_str_bse}_F_0000.CSV"
)

bse_headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
}

bse_resp = requests.get(bse_url, headers=bse_headers, timeout=20)

if bse_resp.status_code != 200:
    raise Exception("❌ BSE Bhavcopy not available")

df_bse = pd.read_csv(io.BytesIO(bse_resp.content))
df_bse.columns = [c.strip() for c in df_bse.columns]

COLUMN_MAP_BSE = {
    "ISIN": "ISIN",
    "TradDt": "Trade_Date",
    "TckrSymb": "Symbol",
    "ClsPric": "Close_Price"
}

missing = [c for c in COLUMN_MAP_BSE if c not in df_bse.columns]
if missing:
    raise Exception(f"❌ BSE missing columns: {missing}")

df_bse_final = df_bse[list(COLUMN_MAP_BSE.keys())].rename(columns=COLUMN_MAP_BSE)

ws_bse = sheet.worksheet("BSE")
ws_bse.clear()
ws_bse.update([df_bse_final.columns.tolist()] + df_bse_final.values.tolist())

print(f"✅ BSE uploaded: {len(df_bse_final)} rows")

# =========================
# DONE
# =========================
print("\n🎉 NSE + BSE Bhavcopy Job Completed Successfully")
