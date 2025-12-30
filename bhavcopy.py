import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
SAVE_PATH = "nse_bhavcopy.csv"
MAX_LOOKBACK_DAYS = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

BASE_URL = "https://www.nseindia.com"


# =========================
# TRY DOWNLOAD FOR A DATE
# =========================
def try_download_for_date(session, trade_date):
    date_str = trade_date.strftime("%d%b%Y").upper()

    url = (
        f"https://archives.nseindia.com/content/historical/EQUITIES/"
        f"{trade_date.year}/{trade_date.strftime('%b').upper()}/"
        f"cm{date_str}bhav.csv.zip"
    )

    print(f"Trying: {date_str}")
    resp = session.get(url, timeout=20)

    if resp.status_code != 200:
        return None

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f)

    return df, date_str


# =========================
# MAIN DOWNLOAD LOGIC
# =========================
def download_latest_bhavcopy():
    session = requests.Session()
    session.headers.update(HEADERS)

    # Set NSE cookies
    session.get(BASE_URL, timeout=10)

    today = datetime.today()

    for i in range(1, MAX_LOOKBACK_DAYS + 1):
        trade_date = today - timedelta(days=i)

        # Skip Sundays
        if trade_date.weekday() == 6:
            continue

        result = try_download_for_date(session, trade_date)
        if result:
            df, date_str = result
            df.to_csv(SAVE_PATH, index=False)
            print(f"\n✅ NSE Bhavcopy FOUND for {date_str}")
            print("Saved to:", SAVE_PATH)
            print("Rows:", len(df))
            return df

    raise Exception("Bhavcopy not found in last 10 days")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    try:
        df = download_latest_bhavcopy()
        print(df.head())
    except Exception as e:
        print("❌ ERROR:", e)
