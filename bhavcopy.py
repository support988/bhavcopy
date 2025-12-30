import requests
import pandas as pd
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import io
import zipfile
import os

# ---------------- DATE ----------------
trade_date = datetime.now().strftime("%d-%b-%Y")
bse_date = datetime.now().strftime("%Y%m%d")

# ---------------- GOOGLE AUTH ----------------
service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    service_account_info, scopes=scope
)

gc = gspread.authorize(creds)

sheet = gc.open("Daily_Bhavcopy")
nse_ws = sheet.worksheet("NSE")
bse_ws = sheet.worksheet("BSE")

# ---------------- NSE UDIFF ----------------
session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports",
    "X-Requested-With": "XMLHttpRequest"
}

session.get("https://www.nseindia.com", headers=headers)

nse_url = "https://www.nseindia.com/api/reports"
params = {
    "archives": '[{"name":"CM-UDiFF Common Bhavcopy Final (zip)","type":"daily-reports","category":"capital-market","section":"equities"}]',
    "date": trade_date,
    "type": "equities",
    "mode": "single"
}

resp = session.get(nse_url, headers=headers, params=params)
z = zipfile.ZipFile(io.BytesIO(resp.content))
nse_file = z.namelist()[0]

nse_df = pd.read_csv(z.open(nse_file))[["ISIN", "TradDt", "TckrSymb", "ClsPric"]]

# ---------------- BSE ----------------
bse_url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{bse_date}_F_0000.CSV"
bse_resp = requests.get(bse_url)

bse_raw = pd.read_csv(io.BytesIO(bse_resp.content))
bse_df = bse_raw.rename(columns={
    "ISIN_CODE": "ISIN",
    "TRADE_DATE": "TradDt",
    "SC_NAME": "TckrSymb",
    "CLOSE": "ClsPric"
})[["ISIN", "TradDt", "TckrSymb", "ClsPric"]]

# ---------------- PUSH TO SHEETS ----------------
nse_ws.append_rows(nse_df.values.tolist())
bse_ws.append_rows(bse_df.values.tolist())

print("Bhavcopy updated successfully")
