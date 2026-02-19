import requests
import zipfile
import io
import pandas as pd
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
from playwright.sync_api import sync_playwright

# =========================
# COMMON CONFIG
# =========================
IST = timezone(timedelta(hours=5, minutes=30))
today_ist = datetime.now(IST).date()
trade_date = today_ist - timedelta(days=1)
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
print("\n🚀 Fetching NSE Bhavcopy via Playwright")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="en-IN",
        timezone_id="Asia/Kolkata"
    )
    page = context.new_page()

    print("🌐 Opening NSE homepage...")
    page.goto("https://www.nseindia.com", wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(3000)

    print("🌐 Opening NSE reports page...")
    page.goto("https://www.nseindia.com/all-reports", wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(3000)

    cookies = context.cookies()
    browser.close()

print(f"🍪 Got {len(cookies)} cookies from NSE")

# Build requests session with real browser cookies
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.nseindia.com/all-reports",
    "X-Requested-With": "XMLHttpRequest",
})
for c in cookies:
    session.cookies.set(c["name"], c["value"], domain=c["domain"])

ARCHIVES_PAYLOAD = [{
    "name": "CM-UDiFF Common Bhavcopy Final (zip)",
    "type": "daily-reports",
    "category": "capital-market",
    "section": "equities"
}]

params = {
    "archives": json.dumps(ARCHIVES_PAYLOAD),
    "date": date_str_nse,
    "type": "equities",
    "mode": "single"
}

print("📡 Calling NSE API...")
resp = session.get("https://www.nseindia.com/api/reports", params=params, timeout=30)
content_type = resp.headers.get("Content-Type", "").lower()
print(f"📦 Content-Type: {content_type} | Status: {resp.status_code}")

if "zip" in content_type:
    z = zipfile.ZipFile(io.BytesIO(resp.content))

elif "json" in content_type:
    data = resp.json()
    if not data or "filePath" not in data[0]:
        raise Exception(f"❌ NSE JSON has no filePath: {data}")
    zip_url = "https://archives.nseindia.com" + data[0]["filePath"]
    print(f"📥 Downloading zip from: {zip_url}")
    z = zipfile.ZipFile(io.BytesIO(session.get(zip_url, timeout=30).content))

else:
    raise Exception(f"❌ Unexpected NSE response: status={resp.status_code}, type={content_type}, body={resp.text[:300]}")

csv_name = z.namelist()[0]
df_nse = pd.read_csv(z.open(csv_name))
df_nse.columns = [c.strip() for c in df_nse.columns]

COLUMN_MAP_NSE = {
    "ISIN": "ISIN",
    "TradDt": "Trade_Date",
    "TckrSymb": "Symbol",
    "ClsPric": "Close_Price",
    "SctySrs": "SctySrs"
}

df_nse_final = df_nse[list(COLUMN_MAP_NSE.keys())].rename(columns=COLUMN_MAP_NSE)

ws_nse = sheet.worksheet("NSE")
ws_nse.clear()
df_nse_final = df_nse_final.replace([float("inf"), float("-inf")], "")
df_nse_final = df_nse_final.fillna("")
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
    raise Exception(f"❌ BSE Bhavcopy not available (status: {bse_resp.status_code})")

df_bse = pd.read_csv(io.BytesIO(bse_resp.content))
df_bse.columns = [c.strip() for c in df_bse.columns]

COLUMN_MAP_BSE = {
    "ISIN": "ISIN",
    "TradDt": "Trade_Date",
    "TckrSymb": "Symbol",
    "ClsPric": "Close_Price",
    "SctySrs": "SctySrs"
}

missing = [c for c in COLUMN_MAP_BSE if c not in df_bse.columns]
if missing:
    raise Exception(f"❌ BSE missing columns: {missing}")

df_bse_final = df_bse[list(COLUMN_MAP_BSE.keys())].rename(columns=COLUMN_MAP_BSE)

ws_bse = sheet.worksheet("BSE")
ws_bse.clear()
df_bse_final = df_bse_final.replace([float("inf"), float("-inf")], "")
df_bse_final = df_bse_final.fillna("")
ws_bse.update([df_bse_final.columns.tolist()] + df_bse_final.values.tolist())

print(f"✅ BSE uploaded: {len(df_bse_final)} rows")

# =========================
# DONE
# =========================
print("\n🎉 NSE + BSE Bhavcopy Job Completed Successfully")
