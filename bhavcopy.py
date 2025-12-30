import requests
import zipfile
import io
import json
import pandas as pd
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# =====================================================
# 1. GOOGLE SHEETS AUTH
# =====================================================
creds_dict = json.loads(os.environ["GSHEET_CREDS"])

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
gc = gspread.authorize(creds)

sheet = gc.open("Daily_Bhavcopy")
nse_ws = sheet.worksheet("NSE")
bse_ws = sheet.worksheet("BSE")

# Clear old data
nse_ws.clear()
bse_ws.clear()

# =====================================================
# 2. DATE LOGIC (YESTERDAY, SKIP WEEKENDS)
# =====================================================
d = datetime.today() - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)

trade_date_str = d.strftime("%d-%b-%Y")
trade_date_file = d.strftime("%Y%m%d")

# =====================================================
# 3. NSE – CM-UDiFF COMMON BHAVCOPY (YOUR METHOD)
# =====================================================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports",
    "X-Requested-With": "XMLHttpRequest"
})

# Get cookies
session.get("https://www.nseindia.com", timeout=10)

api_url = "https://www.nseindia.com/api/reports"

params = {
    "archives": json.dumps([{
        "name": "CM-UDiFF Common Bhavcopy Final (zip)",
        "type": "daily-reports",
        "category": "capital-market",
        "section": "equities"
    }]),
    "date": trade_date_str,
    "type": "equities",
    "mode": "single"
}

resp = session.get(api_url, params=params, timeout=20)

# NSE sometimes returns HTML instead of JSON
if "application/json" not in resp.headers.get("Content-Type", ""):
    print("❌ NSE returned non-JSON response")
    print("Status:", resp.status_code)
    print("Headers:", resp.headers.get("Content-Type"))
    print("Body (first 500 chars):", resp.text[:500])
    raise Exception("NSE API blocked the request (HTML response)")

data = resp.json()

if not data.get("data"):
    raise Exception("NSE bhavcopy not available for " + trade_date_str)

zip_path = data["data"][0]["filePath"]
zip_url = "https://www.nseindia.com" + zip_path

zip_resp = session.get(zip_url, timeout=20)
zip_resp.raise_for_status()

z = zipfile.ZipFile(io.BytesIO(zip_resp.content))
csv_name = z.namelist()[0]

with z.open(csv_name) as f:
    nse_raw = pd.read_csv(f)

nse_df = nse_raw.rename(columns={
    "ISIN": "ISIN",
    "TRADING_DATE": "TradDt",
    "SYMBOL": "TckrSymb",
    "CLOSE_PRICE": "ClsPric"
})[["ISIN", "TradDt", "TckrSymb", "ClsPric"]]

# Upload NSE
nse_ws.update([nse_df.columns.tolist()] + nse_df.values.tolist())

# =====================================================
# 4. BSE – CASH MARKET BHAVCOPY (YOUR METHOD)
# =====================================================
bse_url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{trade_date_file}_F_0000.CSV"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
}

bse_resp = requests.get(bse_url, headers=headers, timeout=20)
bse_resp.raise_for_status()

bse_raw = pd.read_csv(io.BytesIO(bse_resp.content))

bse_df = bse_raw.rename(columns={
    "ISIN_CODE": "ISIN",
    "TRADE_DATE": "TradDt",
    "SC_NAME": "TckrSymb",
    "CLOSE": "ClsPric"
})[["ISIN", "TradDt", "TckrSymb", "ClsPric"]]

# Upload BSE
bse_ws.update([bse_df.columns.tolist()] + bse_df.values.tolist())

print("✅ NSE & BSE Bhavcopy updated successfully")
