import datetime
import logging
import time
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

# Setup Logger
logger = logging.getLogger(__name__)


class MFDataFetcher:
    def __init__(self):
        self._all_schemes = None
        self.session = requests.Session()
        # Mimicking curl headers which were proven to work in this environment
        self.headers = {"User-Agent": "curl/8.1.0", "Accept": "*/*", "Connection": "keep-alive"}
        # Define cache directory
        self.cache_dir = Path("data/cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @st.cache_data(ttl=86400, show_spinner=False)  # Cache fund list for 24 hours
    def get_all_schemes(_self):
        """Fetch all available schemes using direct API."""
        if _self._all_schemes and len(_self._all_schemes) > 100:
            return _self._all_schemes

        url = "https://api.mfapi.in/mf"
        for attempt in range(3):
            try:
                response = _self.session.get(url, headers=_self.headers, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        # Transform list of dicts to {code: name} dict
                        _self._all_schemes = {str(item["schemeCode"]): item["schemeName"] for item in data}
                        logger.info(f"Successfully indexed {len(_self._all_schemes)} mutual fund schemes from AMFI.")
                        return _self._all_schemes

                logger.warning(f"Attempt {attempt + 1} to fetch fund index failed (Status: {response.status_code})")
                time.sleep(2)
            except Exception as e:
                logger.error(f"Error building scheme index (Attempt {attempt + 1}): {e}")
                time.sleep(2)
        raise ConnectionError("Unable to load mutual fund index from AMFI. The service may be temporarily down. Please refresh in a few minutes.")

    def search_funds(self, query):
        """Search for funds matching the query string."""
        if not query:
            return {}

        schemes = self.get_all_schemes()
        if not schemes:
            return {}

        results = {}
        # Clean query: lowercase and remove special chars
        clean_query = query.lower().replace("-", " ").replace(",", " ")
        query_parts = clean_query.split()

        for code, name in schemes.items():
            if not code or not name or str(code).strip().lower() == "scheme code":
                continue

            name_str = str(name).lower().replace("-", " ").replace(",", " ")
            if all(part in name_str for part in query_parts):
                results[code] = name

        if not results and len(query_parts) == 1:
            part = query_parts[0]
            for code, name in schemes.items():
                if part in str(name).lower():
                    results[code] = name

        return results

    def _get_cache_path(self, amfi_code):
        """Return the path to the cached file for a fund."""
        return self.cache_dir / f"{amfi_code}.csv"

    def _is_cache_valid(self, cache_path, max_age_hours=12):
        """Check if the cached file exists and is not too old."""
        if not cache_path.exists():
            return False

        file_time = datetime.datetime.fromtimestamp(cache_path.stat().st_mtime)
        now = datetime.datetime.now()
        return (now - file_time).total_seconds() < (max_age_hours * 3600)

    @st.cache_data(ttl=43200, show_spinner=False)  # In-memory cache for 12 hours
    def get_nav_history(_self, amfi_code):
        """Fetch historical NAV using local cache with API fallback."""
        cache_path = _self._get_cache_path(amfi_code)

        # 1. Try Local Persistent Cache
        if _self._is_cache_valid(cache_path):
            try:
                df = pd.read_csv(cache_path)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").set_index("date")
                logger.info(f"Cache Hit for fund {amfi_code}")
                return df
            except Exception as e:
                logger.warning(f"Persistent cache read failed for {amfi_code}: {e}")

        # 2. API Fallback
        logger.info(f"Cache Miss for fund {amfi_code}. Fetching from AMFI API...")
        url = f"https://api.mfapi.in/mf/{amfi_code}"
        for attempt in range(3):
            try:
                response = _self.session.get(url, headers=_self.headers, timeout=20)

                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "SUCCESS" and data.get("data"):
                        df = pd.DataFrame(data["data"])
                        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
                        df["date"] = pd.to_datetime(df["date"], dayfirst=True)

                        # Save to Persistent Cache
                        df.to_csv(cache_path, index=False)
                        logger.info(f"Successfully fetched and cached fund {amfi_code}.")

                        df = df.sort_values("date").set_index("date")
                        return df

                logger.warning(f"Attempt {attempt + 1} to fetch NAV for {amfi_code} failed (Status: {response.status_code})")
                time.sleep(1.5)
            except Exception as e:
                logger.error(f"API fetch error for {amfi_code} (Attempt {attempt + 1}): {e}")
                time.sleep(2)

        # 3. Final Fallback: If API fails, try expired cache as a last resort
        if cache_path.exists():
            try:
                logger.warning(f"Using EXPIRED cache for fund {amfi_code} due to API unavailability.")
                df = pd.read_csv(cache_path)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").set_index("date")
                return df
            except Exception:
                pass

        raise ConnectionError(f"Unable to fetch NAV for fund {amfi_code}. API down and no cache available.")

    @st.cache_data(ttl=86400, show_spinner=False)
    def get_fund_info(_self, amfi_code):
        """Get detailed fund info using API."""
        url = f"https://api.mfapi.in/mf/{amfi_code}"
        for _attempt in range(3):
            try:
                response = _self.session.get(url, headers=_self.headers, timeout=20)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "SUCCESS" and data.get("meta"):
                        info = data["meta"]
                        return {k.lower(): v for k, v in info.items()}
                time.sleep(1.5)
            except Exception:
                time.sleep(2)
        raise ConnectionError(f"Unable to fetch fund details for {amfi_code}.")

    @st.cache_data(ttl=43200, show_spinner=False)
    def get_benchmark_history(_self, ticker="^NSEI", start_date=None):
        """Fetch benchmark history using yfinance."""
        import yfinance as yf

        try:
            bench = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
            if bench.empty:
                return pd.Series()

            close_data = bench["Close"]
            if isinstance(close_data, pd.DataFrame):
                close_data = close_data.iloc[:, 0]

            return close_data.squeeze()
        except Exception as e:
            logger.error(f"Error fetching benchmark {ticker}: {e}")
            return pd.Series()


if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    fetcher = MFDataFetcher()
    results = fetcher.search_funds("HDFC Top 100")
    print(f"Search results: {results}")
    if results:
        code = list(results.keys())[0]
        nav = fetcher.get_nav_history(code)
        print(f"NAV History for {code}:\n{nav.tail()}")
        info = fetcher.get_fund_info(code)
        print(f"Fund Info: {info.get('scheme_name')}")
