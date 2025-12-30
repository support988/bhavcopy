import requests
import zipfile
import io
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# =======================
# GOOGLE SHEETS SETUP
# =======================
import json, os

creds_json = json.loads(os.environ["GSHEET_CREDS"])

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
gc = gspread.authorize(credentials)

sheet = gc.open(os.environ["SHEET_NAME"])
nse_sheet = sheet.worksheet("NSE")
bse_sheet = sheet.worksheet("BSE")

# =======================
# DATE
# =======================
date = datetime.now().strftime("%d%b%Y").upper()
date_bse = datetime.now().strftime("%d%m%y")

# =======================
# NSE DOWNLOAD (FIXED)
# =======================
session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

session.get("https://www.nseindia.com", headers=headers)

nse_url = f"https://archives.nseindia.com/content/historical/EQUITIES/{datetime.now().year}/{datetime.now().strftime('%b').upper()}/cm{date}bhav.csv.zip"

resp = session.get(nse_url, headers=headers)

z = zipfile.ZipFile(io.BytesIO(resp.content))
csv_name = z.namelist()[0]
nse_df = pd.read_csv(z.open(csv_name))

# =======================
# BSE DOWNLOAD
# =======================
bse_url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date_bse}_CSV.ZIP"
resp = requests.get(bse_url)

z = zipfile.ZipFile(io.BytesIO(resp.content))
csv_name = z.namelist()[0]
bse_df = pd.read_csv(z.open(csv_name))

# =======================
# UPLOAD TO SHEETS
# =======================
nse_s_

