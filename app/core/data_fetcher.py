import datetime
import logging
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
import streamlit as st
from core.logger import get_logger, log_event

# Setup professional logger
logger = get_logger(__name__)

# Production Registry: High-Priority Fund Codes for Cache Pre-warming
# STRICT: Only Direct Plan - Growth options included to minimize tracking error
TOP_FUNDS_REGISTRY = {
    "122639": "Parag Parikh Flexi Cap Fund - Direct Growth",
    "118968": "HDFC Balanced Advantage Fund - Direct Growth",
    "118955": "HDFC Flexi Cap Fund - Direct Growth",
    "118989": "HDFC Mid-Cap Opportunities Fund - Direct Growth",
    "120334": "ICICI Prudential Multi-Asset Fund - Direct Growth",
    "119609": "SBI Equity Hybrid Fund - Direct Growth",
    "120366": "ICICI Prudential Bluechip Fund - Direct Growth",
    "120377": "ICICI Prudential Balanced Advantage - Direct Growth",
    "118778": "Nippon India Small Cap Fund - Direct Growth",
    "119775": "Kotak Midcap Fund - Direct Growth",
    "120344": "ICICI Prudential Value Discovery - Direct Growth",
    "120166": "Kotak Flexicap Fund - Direct Growth",
    "119598": "SBI Large Cap Fund - Direct Growth",
    "118632": "Nippon India Large Cap Fund - Direct Growth",
    "118650": "Nippon India Multi Cap Fund - Direct Growth",
    "120251": "ICICI Prudential Equity & Debt Fund - Direct Growth",
    "119835": "SBI Contra Fund - Direct Growth",
    "118668": "Nippon India Growth Fund - Direct Growth",
    "119727": "SBI Focused Fund - Direct Growth",
    "118834": "Mirae Asset Large & Midcap Fund - Direct Growth",
    "149134": "SBI Balanced Advantage Fund - Direct Growth",
    "118825": "Mirae Asset Large Cap Fund - Direct Growth",
    "119018": "HDFC Large Cap Fund - Direct Growth",
    "119721": "SBI Large & Midcap Fund - Direct Growth",
    "130503": "HDFC Small Cap Fund - Direct Growth",
    "127357": "ICICI Prudential India Opportunities - Direct Growth",
    "125497": "SBI Small Cap Fund - Direct Growth",
    "127042": "Motilal Oswal Midcap Fund - Direct Growth",
    "120465": "Axis Bluechip Fund - Direct Growth",
    "120505": "Axis Midcap Fund - Direct Growth",
    "120158": "Kotak Large & Midcap Fund - Direct Growth",
    "130498": "HDFC Large and Mid Cap Fund - Direct Growth",
    "120596": "ICICI Prudential Large & Mid Cap - Direct Growth",
    "120828": "Quant Small Cap Fund - Direct Growth",
    "118950": "HDFC Focused Fund - Direct Growth",
    "125354": "Axis Small Cap Fund - Direct Growth",
    "119229": "Aditya Birla SL Flexi Cap Fund - Direct Growth",
    "118278": "Canara Robeco Large and Mid Cap - Direct Growth",
    "149185": "Kotak Multicap Fund - Direct Growth",
    "119062": "HDFC Hybrid Equity Fund - Direct Growth",
    "119718": "SBI Flexicap Fund - Direct Growth",
    "119716": "SBI Midcap Fund - Direct Growth",
    "149882": "SBI Multicap Fund - Direct Growth",
    "147946": "Bandhan Small Cap Fund - Direct Growth",
    "148990": "ICICI Prudential Flexicap Fund - Direct Growth",
    "120348": "Invesco India Contra Fund - Direct Growth",
    "119071": "DSP Midcap Fund - Direct Growth",
    "118535": "Franklin India Flexi Cap Fund - Direct Growth",
}


class MFDataFetcher:
    def __init__(self):
        self._all_schemes: Dict[str, str] = {}
        self.session = requests.Session()
        # Mimicking curl headers which were proven to work in this environment
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.mfapi.in",
            "Referer": "https://www.mfapi.in/",
        }
        # Define cache directory
        self.cache_dir = Path("data/cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _sync_from_cloud_cache(self, filename: str) -> bool:
        """Attempt to download pre-warmed cache from the dedicated 'data-cache' branch."""
        try:
            # Using raw.githubusercontent.com to fetch the latest CSV from our orphan branch
            # Repository URL is derived from established professional metadata
            repo_base = "https://raw.githubusercontent.com/convexica/convexlab/data-cache"
            url = f"{repo_base}/data/cache/{filename}"

            target_path = self.cache_dir / filename

            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    with open(target_path, "wb") as f:
                        f.write(response.read())
                    log_event(logger, "CLOUD_CACHE_SYNC", filename=filename, status="Success")
                    return True
        except Exception as e:
            # Silent fallback if cloud sync fails
            logger.debug(f"Cloud cache sync skipped for {filename}: {e}")
        return False

    @st.cache_data(ttl=86400, show_spinner=False)  # Cache fund list for 24 hours
    def get_all_schemes(_self) -> dict[str, str]:
        """Fetch all available schemes using direct API with file cache fallback."""
        if _self._all_schemes and len(_self._all_schemes) > 100:
            return _self._all_schemes

        scheme_cache = _self.cache_dir / "scheme_index.json"

        # 1. Try to load from valid file cache first (if less than 24h old)
        if _self._is_cache_valid(scheme_cache, max_age_hours=24):
            try:
                import json

                with open(scheme_cache, "r", encoding="utf-8") as f:
                    _self._all_schemes = json.load(f)
                if len(_self._all_schemes) > 0:
                    log_event(logger, "INDEX_CACHE_HIT", count=len(_self._all_schemes), source="LocalFile")
                    return _self._all_schemes
            except Exception as e:
                logger.warning(f"Failed to read scheme cache: {e}")

        # 2. Fetch from API with Exponential Backoff
        url = "https://api.mfapi.in/mf"
        max_attempts = 5
        base_delay = 2

        for attempt in range(max_attempts):
            try:
                # Add jitter to avoid synchronized stampede from cloud nodes
                import random

                delay = (base_delay**attempt) + random.random()
                if attempt > 0:
                    time.sleep(delay)

                response = _self.session.get(url, headers=_self.headers, timeout=(15 if attempt < 2 else 30))

                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        _self._all_schemes = {str(item["schemeCode"]): item["schemeName"] for item in data}

                        # Persist to file cache
                        import json

                        with open(scheme_cache, "w", encoding="utf-8") as f:
                            json.dump(_self._all_schemes, f)

                        log_event(logger, "INDEX_SYNC_SUCCESS", count=len(_self._all_schemes), source="AMFI")
                        return _self._all_schemes

                elif response.status_code == 429:
                    logger.warning(f"Rate limited by AMFI (429) on attempt {attempt + 1}")
                else:
                    logger.warning(f"AMFI API returned status {response.status_code} on attempt {attempt + 1}")

            except requests.exceptions.Timeout:
                logger.warning(f"AMFI API timeout on attempt {attempt + 1}")
            except Exception as e:
                log_event(logger, "INDEX_SYNC_ERROR", level="error", attempt=attempt + 1, error=str(e))

        # 3. Last resort: use expired file cache if exists
        if scheme_cache.exists():
            try:
                import json

                with open(scheme_cache, "r", encoding="utf-8") as f:
                    _self._all_schemes = json.load(f)
                if len(_self._all_schemes) > 0:
                    logger.warning("Using EXPIRED scheme index cache due to API failure.")
                    return _self._all_schemes
            except Exception:
                pass

        raise ConnectionError("Unable to load mutual fund index from AMFI. The service may be temporarily down. Please refresh in a few minutes.")

    def search_funds(self, query: str) -> dict[str, str]:
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
    def get_nav_history(_self, amfi_code: str) -> pd.DataFrame:
        """Fetch historical NAV using local cache with API fallback."""
        cache_path = _self._get_cache_path(amfi_code)

        # 1. Try Local Persistent Cache
        if _self._is_cache_valid(cache_path):
            try:
                df = pd.read_csv(cache_path)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").set_index("date")
                log_event(logger, "CACHE_HIT", code=amfi_code, source="LocalCSV")
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
                        log_event(logger, "API_FETCH_SUCCESS", code=amfi_code, source="AMFI")

                        df = df.sort_values("date").set_index("date")
                        return df

                log_event(logger, "API_FETCH_FAILURE", level="warning", code=amfi_code, attempt=attempt + 1, status=response.status_code)
                time.sleep(1.5)
            except Exception as e:
                log_event(logger, "API_FETCH_ERROR", level="error", code=amfi_code, attempt=attempt + 1, error=str(e))
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
    def get_fund_info(_self, amfi_code: str) -> dict[str, Any]:
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
    def get_benchmark_history(_self, ticker: str = "^NSEI", start_date: Optional[datetime.date] = None) -> pd.Series:
        """Fetch benchmark history using yfinance with timezone normalization."""
        import yfinance as yf

        try:
            bench = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
            if bench.empty:
                log_event(logger, "BENCHMARK_EMPTY", ticker=ticker, status="SUCCESS")
                return pd.Series()

            # yfinance 0.2.x can return MultiIndex: (Close, ^NSEI)
            close_data = bench["Close"]
            if isinstance(close_data, pd.DataFrame):
                close_data = close_data.iloc[:, 0]

            # Institutional-Grade Normalization: Force Timezone Naive
            # AMFI data is naive, so we must match it for consistent joins.
            close_data.index = pd.to_datetime(close_data.index).tz_localize(None)

            log_event(logger, "BENCHMARK_SYNC_SUCCESS", ticker=ticker, data_points=len(close_data))
            result = close_data.squeeze()
            if isinstance(result, pd.Series):
                return result
            return pd.Series(result)
        except Exception as e:
            log_event(logger, "BENCHMARK_FETCH_ERROR", level="error", ticker=ticker, error=str(e))
            return pd.Series()

    @st.cache_data(ttl=86400, show_spinner=False)
    def get_current_risk_free_rate(_self) -> float:
        """
        Fetches the current Indian Risk-Free Rate (91D T-Bill).
        Primary Source: TradingEconomics (Real-time Market Yield).
        Fallback: Institutional Baseline (6.5%).
        """
        import re

        url = "https://tradingeconomics.com/india/3-month-bill-yield"
        try:
            response = _self.session.get(url, headers=_self.headers, timeout=10)
            if response.status_code == 200:
                # Optimized regex for JSON/Meta extraction (Stable and Precise)
                match = re.search(r'"Value":\s*(\d+\.\d+)', response.text, re.I)
                if not match:
                    match = re.search(r'"Last":\s*(\d+\.\d+)', response.text, re.I)

                if match:
                    rate = float(match.group(1))
                    log_event(logger, "RF_FETCH_SUCCESS", source="TradingEconomics", rate=rate)
                    return rate / 100
        except Exception as e:
            log_event(logger, "RF_FETCH_ERROR", level="warning", source="TradingEconomics", error=str(e))

        # Final Institutional Fallback (Zero-Fail Policy)
        log_event(logger, "RF_FETCH_FALLBACK", level="info", source="StaticBaseline", rate=0.065)
        return 0.065  # 6.5% - Reliable benchmark for Indian debt markets


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
