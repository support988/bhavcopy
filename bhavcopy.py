import requests
import zipfile
import io
import pandas as pd
import datetime
import sys

# ---------------- CONFIG ----------------
NSE_API = "https://www.nseindia.com/api/reports"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
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

# First call NSE homepage (mandatory)
session.get("https://www.nseindia.com", timeout=10)

# ---------------- API CALL ----------------
params = {
    "archives": str(ARCHIVES_PAYLOAD).replace("'", '"'),
    "date": date_str,
    "type": "equities",
    "mode": "single"
}

resp = session.get(NSE_API, params=params, timeout=15)

# ---------------- VALIDATION ----------------
if resp.headers.get("Content-Type", "").startswith("text/html"):
    print("❌ NSE returned HTML — bhavcopy not released yet")
    sys.exit(1)

data = resp.json()

if not data or "filePath" not in data[0]:
    print("❌ NSE API returned no filePath")
    sys.exit(1)

zip_url = "https://archives.nseindia.com" + data[0]["filePath"]
print("⬇ Downloading:", zip_url)

# ---------------- DOWNLOAD ZIP ----------------
zip_resp = session.get(zip_url, timeout=20)
zip_resp.raise_for_status()

z = zipfile.ZipFile(io.BytesIO(zip_resp.content))
csv_name = z.namelist()[0]

# ---------------- READ CSV ----------------
df = pd.read_csv(z.open(csv_name))

# ---------------- REQUIRED HEADERS ----------------
final_df = df[["ISIN", "TRAD_DT", "TCKR_SYMB", "CLS_PRC"]].rename(columns={
    "TRAD_DT": "TradDt",
    "TCKR_SYMB": "TckrSymb",
    "CLS_PRC": "ClsPric"
})

final_df.to_csv("NSE_Bhavcopy.csv", index=False)
print("✅ NSE Bhavcopy saved successfully")
