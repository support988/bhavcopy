import requests
import zipfile
import io
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json, os

# =======================
# GOOGLE SHEETS AUTH
# =======================
creds_dict = json.loads(os.environ["GSHEET_CREDS"])

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
gc = gspread.authorize(credentials)

sheet = gc.open("Daily_Bhavcopy")
nse_sheet = sheet.worksheet("NSE")
bse_sheet = sheet.worksheet("BSE")

# =======================
# DATE
# =======================
today = datetime.now()
nse_date = today.strftime("%d%b%Y").upper()
bse_date = today.strftime("%d%m%y")

# =======================
# NSE BHAVCOPY (ANTI-BOT FIX)
# =======================
session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

session.get("https://www.nseindia.com", headers=headers)

nse_url = (
    f"https://archives.nseindia.com/content/historical/EQUITIES/"
    f"{today.year}/{today.strftime('%b').upper()}/cm{nse_date}bhav.csv.zip"
)

resp = session.get(nse_url, headers=headers, timeout=30)
resp.raise_for_status()

z = zipfile.ZipFile(io.BytesIO(resp.content))
nse_csv = z.namelist()[0]
nse_df = pd.read_csv(z.open(nse_csv))

# =======================
# BSE BHAVCOPY
# =======================
bse_url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{bse_date}_CSV.ZIP"
resp = requests.get(bse_url, timeout=30)
resp.raise_for_status()

z = zipfile.ZipFile(io.BytesIO(resp.content))
bse_csv = z.namelist()[0]
bse_df = pd.read_csv(z.open(bse_csv))

# =======================
# UPLOAD TO GOOGLE SHEETS
# =======================
nse_sheet.clear()
nse_sheet.update([nse_df.columns.tolist()] + nse_df.values.tolist())

bse_sheet.clear()
bse_sheet.update([bse_df.columns.tolist()] + bse_df.values.tolist())

print("✅ NSE & BSE Bhavcopy uploaded successfully")
