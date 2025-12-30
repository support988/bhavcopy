import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
SAVE_PATH = "nse_bhavcopy.csv"

# NSE needs proper headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

BASE_URL = "https://www.nseindia.com"


# =========================
# GET LAST TRADING DAY
# =========================
def get_last_trading_day():
    d = datetime.today()
    if d.weekday() == 0:   # Monday → Friday
        d = d - timedelta(days=3)
    elif d.weekday() == 6: # Sunday → Friday
        d = d - timedelta(days=2)
    else:
        d = d - timedelta(days=1)
    return d


# =========================
# DOWNLOAD BHAVCOPY
# =========================
def download_nse_bhavcopy():
    trade_date = get_last_trading_day()
    date_str = trade_date.strftime("%d%b%Y").upper()

    url = f"https://archives.nseindia.com/content/historical/EQUITIES/{trade_date.year}/{trade_date.strftime('%b').upper()}/cm{date_str}bhav.csv.zip"

    session = requests.Session()
    session.headers.update(HEADERS)

    # First hit NSE homepage to set cookies
    session.get(BASE_URL, timeout=10)

    print(f"Downloading NSE Bhavcopy for {date_str}...")
    resp = session.get(url, timeout=20)

    if resp.status_code != 200:
        raise Exception(f"Download failed. HTTP {resp.status_code}")

    # Extract ZIP
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f)

    df.to_csv(SAVE_PATH, index=False)
    print("✅ Bhavcopy saved successfully:", SAVE_PATH)
    print("Rows:", len(df))
    return df


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    try:
        df = download_nse_bhavcopy()
        print(df.head())
    except Exception as e:
        print("❌ ERROR:", e)
