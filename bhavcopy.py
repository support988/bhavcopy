import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com",
}

SAVE_PATH = "nse_bhavcopy.csv"
BASE_URL = "https://archives.nseindia.com/content/udiff/"
LOOKBACK_DAYS = 15


def download_latest_udiff_bhavcopy():
    session = requests.Session()
    session.headers.update(HEADERS)

    for i in range(LOOKBACK_DAYS):
        date = datetime.utcnow() - timedelta(days=i)
        date_str = date.strftime("%Y%m%d")

        file_name = f"CM_UDiFF_Common_Bhavcopy_{date_str}_FINAL.zip"
        url = BASE_URL + file_name

        print(f"Trying: {file_name}")
        resp = session.get(url, timeout=20)

        if resp.status_code != 200:
            continue

        # ZIP CONFIRMED
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_files = [f for f in z.namelist() if f.endswith(".csv")]
            if not csv_files:
                continue

            with z.open(csv_files[0]) as f:
                df = pd.read_csv(f)

        df.to_csv(SAVE_PATH, index=False)
        print(f"\n✅ NSE Bhavcopy downloaded successfully: {date_str}")
        print("Rows:", len(df))
        return df

    raise Exception("No NSE Bhavcopy found in last 15 days")


if __name__ == "__main__":
    download_latest_udiff_bhavcopy()
