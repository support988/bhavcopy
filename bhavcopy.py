import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
SAVE_PATH = "nse_bhavcopy.csv"
MAX_LOOKBACK_DAYS = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE_URL = "https://www.nseindia.com"


# =========================
# GET NSE SERVER DATE
# =========================
def get_nse_server_date(session):
    r = session.get(BASE_URL, timeout=10)
    date_header = r.headers.get("Date")
    if not date_header:
        raise Exception("Unable to fetch NSE server date")

    return datetime.strptime(date_header, "%a, %d %b %Y %H:%M:%S %Z")


# =========================
# TRY DOWNLOAD FOR A DATE
# =========================
def try_download(session, trade_date):
    date_str = trade_date.strftime("%d%b%Y").upper()

    url = (
        f"https://archives.nseindia.com/content/historical/EQUITIES/"
        f"{trade_date.year}/{trade_date.strftime('%b').upper()}/"
        f"cm{date_str}bhav.csv.zip"
    )

    print(f"Trying: {date_str}")
    r = session.get(url, timeout=20)

    if r.status_code != 200:
        return None

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_file = z.namelist()[0]
        with z.open(csv_file) as f:
            df = pd.read_csv(f)

    return df, date_str


# =========================
# MAIN LOGIC
# =========================
def download_latest_bhavcopy():
    session = requests.Session()
    session.headers.update(HEADERS)

    # Establish NSE cookies
    session.get(BASE_URL, timeout=10)

    nse_today = get_nse_server_date(session).date()
    print("NSE Server Date:", nse_today)

    for i in range(1, MAX_LOOKBACK_DAYS + 1):
        trade_date = nse_today - timedelta(days=i)

        # Skip Sundays
        if trade_date.weekday() == 6:
            continue

        result = try_download(session, trade_date)
        if result:
            df, date_str = result
            df.to_csv(SAVE_PATH, index=False)
            print(f"\n✅ Bhavcopy downloaded for {date_str}")
            print("Rows:", len(df))
            return df

    raise Exception("Bhavcopy not available in last 15 trading days")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    try:
        download_latest_bhavcopy()
    except Exception as e:
        print("❌ ERROR:", e)
