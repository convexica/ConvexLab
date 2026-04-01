from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import streamlit as st
from core.logger import get_logger

# Initialize professional logger
logger = get_logger(__name__)


class MFAnalytics:
    """
    A comprehensive financial analytics engine for Mutual Fund performance analysis.

    Provides vectorized calculations for risk-adjusted returns, benchmark comparisons,
    and rolling return distributions.
    """

    def __init__(self, risk_free_rate: float = 0.053):
        """
        Initialize the analytics engine.

        Args:
            risk_free_rate (float): The annual risk-free rate (e.g., 0.053 for 5.3%).
        """
        self.rf = risk_free_rate

    @st.cache_data(show_spinner=False)
    def calculate_cagr(_self, nav_series: pd.Series) -> float:
        """
        Calculate the Compound Annual Growth Rate (CAGR).

        Args:
            nav_series (pd.Series): Time series of Net Asset Values.

        Returns:
            float: The annualized CAGR. Returns 0.0 if data is insufficient.
        """
        if nav_series.empty or len(nav_series) < 2:
            return 0.0

        start_val = nav_series.iloc[0]
        end_val = nav_series.iloc[-1]

        years = (nav_series.index[-1] - nav_series.index[0]).days / 365.25
        if years <= 0:
            return 0.0

        return (end_val / start_val) ** (1 / years) - 1

    @st.cache_data(show_spinner=False)
    def calculate_rolling_returns(_self, nav_series: pd.Series, window_years: int = 1) -> pd.Series:
        """
        Calculate rolling returns for a given time window.

        Args:
            nav_series (pd.Series): Time-series of NAV values.
            window_years (int): The rolling window length in years.

        Returns:
            pd.Series: A series of rolling annualized returns.
        """
        if nav_series.empty:
            return pd.Series(dtype=float)

        # Approx 252 trading days in a year
        window = int(window_years * 252)
        if len(nav_series) < window:
            return pd.Series(dtype=float)

        return (nav_series / nav_series.shift(window)) ** (1 / window_years) - 1

    def calculate_downside_deviation(self, nav_series: pd.Series) -> float:
        """
        Calculate the Annualized Downside Deviation (Semi-variance).

        Measures the volatility of negative returns only, providing a more
        accurate risk metric for asymmetric return distributions.

        Args:
            nav_series (pd.Series): Time-series of NAV values.

        Returns:
            float: Annualized downside deviation.
        """
        if nav_series.empty or len(nav_series) < 2:
            return 0.0

        returns = nav_series.pct_change(fill_method=None).dropna()
        if returns.empty:
            return 0.0

        # Filter only negative returns
        negative_returns = returns[returns < 0]
        sum_sq_negative = (negative_returns**2).sum()

        # Daily downside risk
        downside_risk_daily = np.sqrt(sum_sq_negative / len(returns))
        return downside_risk_daily * np.sqrt(252)

    @st.cache_data(show_spinner=False)
    def calculate_risk_metrics(_self, nav_series: pd.Series, rf_rate: Optional[float] = None) -> Dict[str, float]:
        """
        Compute a comprehensive set of risk-adjusted return metrics.

        Includes Volatility, Sharpe Ratio, Sortino Ratio, Calmar Ratio,
        Omega Ratio, Hurst Exponent, and CAGR.

        Args:
            nav_series (pd.Series): Time-series of NAV values.
            rf_rate (float, optional): Custom risk-free rate. Defaults to instance rf.

        Returns:
            Dict[str, float]: A dictionary containing calculated metrics.
        """
        rf = rf_rate if rf_rate is not None else _self.rf

        if nav_series.empty or len(nav_series) < 2:
            return {}

        returns = nav_series.pct_change(fill_method=None).dropna()
        if returns.empty:
            return {}

        # Basic statistics
        volatility = returns.std() * np.sqrt(252)
        mean_annual_return = returns.mean() * 252
        cagr = _self.calculate_cagr(nav_series)

        # Sharpe Ratio
        excess_return = mean_annual_return - rf
        sharpe = excess_return / volatility if volatility != 0 else 0

        # Sortino Ratio
        downside_dev = _self.calculate_downside_deviation(nav_series)
        sortino = excess_return / downside_dev if downside_dev != 0 else 0

        # Calmar Ratio
        _, max_dd = _self.calculate_drawdowns(nav_series)
        calmar = (cagr / abs(max_dd)) if max_dd != 0 else 0

        # Omega Ratio (Threshold set at rf/252)
        threshold = rf / 252
        gains = returns[returns > threshold].sum()
        losses = abs(returns[returns <= threshold].sum())
        omega = gains / losses if losses != 0 else 0

        # Behavioral Metrics
        hurst = _self.calculate_hurst(nav_series)

        return {
            "volatility": volatility,
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "downside_deviation": downside_dev,
            "calmar_ratio": calmar,
            "omega_ratio": omega,
            "hurst_exponent": hurst,
            "cagr": cagr,
        }

    def calculate_hurst(self, nav_series: pd.Series) -> float:
        """
        Calculate the Hurst Exponent (H) using Rescaled Range (R/S) methodology.

        Interpretation:
            H = 0.5: Random Walk (Brownian Motion).
            H < 0.5: Mean-Reverting (Anti-persistent).
            H > 0.5: Trending (Persistent).

        Args:
            nav_series (pd.Series): Time-series of NAV values.

        Returns:
            float: The Hurst Exponent. Defaults to 0.5 if data < 100 points.
        """
        if nav_series.empty or len(nav_series) < 100:
            return 0.5

        lags = range(2, 20)
        vals = nav_series.to_numpy()

        # Variance of differences across different lags
        tau = [np.sqrt(np.std(vals[lag:] - vals[:-lag])) for lag in lags]

        # Slope of the log plot log(lags) vs log(tau)
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
        return poly[0] * 2.0

    @st.cache_data(show_spinner=False)
    def calculate_drawdowns(_self, nav_series: pd.Series) -> Tuple[pd.Series, float]:
        """
        Generate the drawdown series and calculate Maximum Drawdown.

        Args:
            nav_series (pd.Series): Time-series of NAV values.

        Returns:
            Tuple[pd.Series, float]: (Drawdown series, Max Drawdown).
        """
        if nav_series.empty:
            return pd.Series(dtype=float), 0.0

        rolling_max = nav_series.cummax()
        drawdown = (nav_series - rolling_max) / rolling_max
        return drawdown, float(drawdown.min())

    @st.cache_data(show_spinner=False)
    def calculate_capture_ratios(_self, fund_nav: pd.Series, benchmark_nav: Union[pd.Series, pd.DataFrame]) -> Dict[str, float]:
        """
        Calculate Upside and Downside Capture Ratios relative to a benchmark.

        Args:
            fund_nav (pd.Series): Time-series of Fund NAV.
            benchmark_nav (pd.Series|pd.DataFrame): Time-series of Benchmark values.

        Returns:
            Dict[str, float]: {'upside': percentage, 'downside': percentage}.
        """
        # Ensure 1D series
        bench_series: pd.Series
        if isinstance(benchmark_nav, pd.DataFrame):
            bench_series = benchmark_nav.iloc[:, 0]
        else:
            bench_series = benchmark_nav

        df = pd.DataFrame({"fund": fund_nav, "bench": bench_series}).dropna()
        if df.empty:
            return {"upside": 0.0, "downside": 0.0}

        # Monthly resampled returns
        monthly_df = df.resample("ME").last().pct_change(fill_method=None).dropna()

        upside_bench = monthly_df[monthly_df["bench"] > 0]
        downside_bench = monthly_df[monthly_df["bench"] <= 0]

        upside_ratio = (upside_bench["fund"].mean() / upside_bench["bench"].mean()) if not upside_bench.empty else 0
        downside_ratio = (downside_bench["fund"].mean() / downside_bench["bench"].mean()) if not downside_bench.empty else 0

        return {"upside": upside_ratio * 100, "downside": downside_ratio * 100}

    @st.cache_data(show_spinner=False)
    def calculate_stress_performance(_self, fund_nav: pd.Series, bench_nav: pd.Series) -> pd.DataFrame:
        """
        Calculate and return peak-to-trough performance during historical market stress events.
        Dynamically handles funds that might not have existed during older crises (like 2008).
        """
        scenarios = [
            {"Name": "2024-25 Market Correction", "Start": "2024-09-27", "End": "2025-04-01"},
            {"Name": "2022 Global Tightening", "Start": "2021-10-18", "End": "2022-06-17"},
            {"Name": "COVID-19 Crash", "Start": "2020-02-19", "End": "2020-03-23"},
            {"Name": "2018 Broad Market Correction", "Start": "2018-01-15", "End": "2018-10-26"},
            {"Name": "2008 Financial Crisis", "Start": "2008-01-08", "End": "2009-03-09"},
        ]

        df = pd.DataFrame({"Fund": fund_nav, "Benchmark": bench_nav}).dropna()
        if df.empty:
            return pd.DataFrame()

        results = []
        for cr in scenarios:
            s_dt = pd.to_datetime(cr["Start"])
            e_dt = pd.to_datetime(cr["End"])

            # Data Validation: Only run if fund and bench have history before crisis starts
            # Adding a tiny buffer (30 days) to prevent edge cases for newly launched funds
            if df.index[0] > (s_dt - pd.Timedelta(days=30)):
                continue

            try:
                # Find accurate trading days closest to the target bounds
                start_mask = df.index >= s_dt
                if not start_mask.any():
                    continue
                start_real = df.index[start_mask][0]

                end_mask = df.index <= e_dt
                if not end_mask.any():
                    continue
                end_real = df.index[end_mask][-1]

                if start_real >= end_real:
                    continue

                f_start, f_end = df.loc[start_real, "Fund"], df.loc[end_real, "Fund"]
                b_start, b_end = df.loc[start_real, "Benchmark"], df.loc[end_real, "Benchmark"]

                f_ret = (f_end / f_start) - 1
                b_ret = (b_end / b_start) - 1

                # Calculate Downside Capture. If benchmark positive (rare in crisis), report N/A
                capture = (f_ret / b_ret) if b_ret < 0 else None

                results.append(
                    {"Crisis": cr["Name"], "Period": f"{start_real.strftime('%b %Y')} - {end_real.strftime('%b %Y')}", "Fund Drop": f_ret, "Benchmark Drop": b_ret, "Capture Ratio": capture}
                )
            except Exception as e:
                logger.warning(f"Error calculating stress scenario for {cr['Name']}: {e}")
                continue

        # Sort by most recent crisis first
        return pd.DataFrame(results)

    @st.cache_data(show_spinner=False)
    def calculate_alpha_beta(_self, fund_nav: pd.Series, benchmark_nav: Union[pd.Series, pd.DataFrame], rf_rate: Optional[float] = None) -> Dict[str, float]:
        """
        Calculate Alpha and Beta using Linear Regression on daily excess returns.

        Formula: Fund_Excess = Alpha + Beta * Bench_Excess

        Args:
            fund_nav (pd.Series): Fund NAV series.
            benchmark_nav (pd.Series|pd.DataFrame): Benchmark price series.
            rf_rate (float, optional): Risk-free rate. Defaults to instance rf.

        Returns:
            Dict[str, float]: Regression outputs (Alpha, Beta, R-Squared, etc.).
        """
        rf = rf_rate if rf_rate is not None else _self.rf
        bench_series: pd.Series
        if isinstance(benchmark_nav, pd.DataFrame):
            bench_series = benchmark_nav.iloc[:, 0]
        else:
            bench_series = benchmark_nav

        df = pd.DataFrame({"fund": fund_nav, "bench": bench_series}).dropna()
        if len(df) < 20:
            return {"alpha": 0.0, "beta": 0.0, "r_squared": 0.0}

        f_ret = df["fund"].pct_change(fill_method=None).dropna()
        b_ret = df["bench"].pct_change(fill_method=None).dropna()

        daily_rf = rf / 252
        f_excess = f_ret - daily_rf
        b_excess = b_ret - daily_rf

        # CAPM Regression
        beta, alpha_daily = np.polyfit(b_excess, f_excess, 1)
        alpha_annual = alpha_daily * 252

        # Stats
        correlation = np.corrcoef(b_excess, f_excess)[0, 1]
        r_squared = correlation**2

        # Information Ratio (Institutional Daily-Annualized)
        active_returns = f_ret - b_ret
        tracking_error = active_returns.std() * np.sqrt(252)
        info_ratio = (active_returns.mean() * 252) / tracking_error if tracking_error != 0 else 0

        # Batting Average (Institutional Monthly Consistency)
        monthly_df = df.resample("ME").last().pct_change(fill_method=None).dropna()
        if not monthly_df.empty:
            batting_avg = (monthly_df["fund"] > monthly_df["bench"]).mean() * 100
        else:
            batting_avg = 0.0

        return {"alpha": alpha_annual, "beta": beta, "r_squared": r_squared, "info_ratio": info_ratio, "batting_average": batting_avg}

    def calculate_fund_multiplier(self, nav_series: pd.Series) -> float:
        """Calculate asset growth multiplier (e.g., 2.5x)."""
        if nav_series.empty:
            return 1.0
        return float(nav_series.iloc[-1] / nav_series.iloc[0])

    def calculate_calendar_returns(self, nav_series: pd.Series) -> pd.Series:
        """Calculate annual returns for each calendar year."""
        if nav_series.empty:
            return pd.Series(dtype=float)

        yearly_nav = nav_series.resample("YE").last()
        returns = yearly_nav.pct_change(fill_method=None)

        # Handle first year (possibly partial)
        if not yearly_nav.empty:
            returns.iloc[0] = (yearly_nav.iloc[0] / nav_series.iloc[0]) - 1

        dt_index = pd.DatetimeIndex(returns.index)
        returns.index = pd.Index(dt_index.year)
        return returns

    @st.cache_data(show_spinner=False)
    def calculate_rolling_return_profile(_self, nav_series: pd.Series, bench_nav_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """
        Generate statistical profiles for standard rolling horizons including Outperformance.
        Updated: 2026-04-01 (v2.3.0 Signature Sync)
        """
        profile: Dict[str, Any] = {}
        horizons = {1: "1 Year", 3: "3 Years", 5: "5 Years", 7: "7 Years", 10: "10 Years"}

        for yrs, label in horizons.items():
            rolling = _self.calculate_rolling_returns(nav_series, window_years=yrs).dropna()
            if rolling.empty:
                profile[label] = None
                continue

            # Calculate Outperformance frequency if benchmark is provided
            outperf_pct = 0.0
            if bench_nav_series is not None:
                bench_rolling = _self.calculate_rolling_returns(bench_nav_series, window_years=yrs).dropna()
                # Align both series to find common periods
                common_ix = rolling.index.intersection(bench_rolling.index)
                if not common_ix.empty:
                    outperf_pct = (rolling[common_ix] > bench_rolling[common_ix]).mean()

            profile[label] = {
                "Minimum Return": rolling.min(),
                "Median Return": rolling.median(),
                "Maximum Return": rolling.max(),
                "Outperformance": outperf_pct,
                "% times returns   < -20%": (rolling < -0.20).mean(),
                "% times returns   -20% to -10%": ((rolling >= -0.20) & (rolling < -0.10)).mean(),
                "% times returns   -10% to 0%": ((rolling >= -0.10) & (rolling < 0.00)).mean(),
                "% times returns    0% to 5%": ((rolling >= 0.00) & (rolling < 0.05)).mean(),
                "% times returns    5% to 10%": ((rolling >= 0.05) & (rolling < 0.10)).mean(),
                "% times returns   10% to 15%": ((rolling >= 0.10) & (rolling < 0.15)).mean(),
                "% times returns   15% to 20%": ((rolling >= 0.15) & (rolling < 0.20)).mean(),
                "% times returns   > 20%": (rolling >= 0.20).mean(),
            }
        return profile

    def get_periodic_metrics(self, series: pd.Series, years: int, bench_series: Optional[pd.Series] = None) -> Tuple[Optional[float], Optional[float], Optional[Dict[str, float]]]:
        """
        Calculates performance and risk metrics for a specific multi-year window.
        Moved from UI layer to Engine for SOLID architectural compliance.
        """
        if series.empty:
            return None, None, None
        try:
            target_date = series.index[-1] - pd.DateOffset(years=years)
            subset = series.loc[series.index >= target_date]
            if len(subset) < 20:
                return None, None, None

            start_val = subset.iloc[0]
            end_val = series.iloc[-1]
            ann_ret = (end_val / start_val) ** (1 / years) - 1

            # Annualized Volatility
            daily_rets = subset.pct_change(fill_method=None).dropna()
            ann_vol = daily_rets.std() * np.sqrt(252)

            ratios = {}
            if bench_series is not None and not bench_series.empty:
                b_subset = bench_series.loc[bench_series.index >= target_date]
                if len(b_subset) >= 20:
                    ab = self.calculate_alpha_beta(subset, b_subset)
                    rm = self.calculate_risk_metrics(subset)
                    cap = self.calculate_capture_ratios(subset, b_subset)
                    _, mdd = self.calculate_drawdowns(subset)

                    ratios = {
                        "Alpha": ab["alpha"],
                        "Beta": ab["beta"],
                        "R-Squared": ab["r_squared"],
                        "InfoRatio": ab.get("info_ratio", 0),
                        "BattingAvg": ab.get("batting_average", 0),
                        "Sharpe": rm.get("sharpe_ratio", 0),
                        "Sortino": rm.get("sortino_ratio", 0),
                        "Volatility": ann_vol,
                        "MaxDrawdown": mdd,
                        "Calmar": rm.get("calmar_ratio", 0),
                        "Omega": rm.get("omega_ratio", 0),
                        "Hurst": rm.get("hurst_exponent", 0.5),
                        "Upside": cap["upside"],
                        "Downside": cap["downside"],
                        "CaptureRatio": (cap["upside"] / cap["downside"]) if cap.get("downside", 0) != 0 else 0,
                    }

            return ann_ret, ann_vol, ratios
        except Exception:
            return None, None, None

    def get_monthly_returns(self, fund_nav: pd.Series, bench_nav: pd.Series) -> pd.DataFrame:
        """Alignment and resampling logic for monthly comparative returns."""
        df = pd.DataFrame({"Fund": fund_nav, "Bench": bench_nav}).dropna()
        if df.empty:
            return pd.DataFrame()
        result = df.resample("ME").last().pct_change(fill_method=None).dropna()
        return result if isinstance(result, pd.DataFrame) else pd.DataFrame(result)

    def generate_ai_report_markdown(self, fund_name: str, benchmark_name: str, deep_metrics: List[dict], rolling_profiles: dict, stress_df: Optional[pd.DataFrame] = None) -> str:
        """Generates a structured professional markdown report designed for AI synthesis."""
        if stress_df is None:
            stress_df = pd.DataFrame()

        report = [
            "# ROLE",
            "You are a Senior Quantitative Investment Analyst specializing in Portfolio Management "
            "and Fiduciary Oversight. Your expertise lies in decoding complex fund "
            "forensics to provide actionable risk-adjusted performance evaluations for investment "
            "committees.",
            "\n# TASK",
            f"Analyze the provided quantitative data for the mutual fund: **{fund_name}**. You "
            "must evaluate its performance through a rigorous analytical lens, focusing "
            "specifically on defensive characteristics and structural return consistency relative "
            f"to its primary benchmark: **{benchmark_name}**.",
            "\n# ANALYTICAL PARAMETERS",
            "1. **Capital Protection & Downside Analytics:**",
            "    * **Drawdown Profile:** Analyze Magnitude, Time underwater, and Recovery.",
            "    * **Capture Ratios:** Evaluate Upside/Downside; focus on Downside Efficiency.",
            "2. **Return Consistency (Rolling Profile):**",
            "    * **Rolling Returns:** Analyze 3Y and 5Y windows for persistence.",
            "    * **Outperformance Probability:** Calculate frequency of benchmark-beating periods.",
            "3. **Risk-Adjusted Efficiency:**",
            "    * Synthesize Sharpe, Sortino, and Information Ratios to determine alpha justification vs downside volatility.",
            "\n# DATA ASSETS",
            "## 🏆 Performance Grid (Compounding & Efficiency)",
            "| Period | Sharpe | Sortino | Info Ratio | Alpha | Beta | Batting Avg | Up/Down Efficiency | Upside | Downside |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]

        # Add all horizons from deep_metrics
        for m in deep_metrics:
            report.append(
                f"| {m.get('Period', 'N/A')} | {m.get('Sharpe', 0):.2f} | {m.get('Sortino', 0):.2f} | {m.get('Info Ratio', 0):.2f} | "
                f"{m.get('Jensen Alpha', 0):.1%} | {m.get('Beta', 0):.2f} | {m.get('Batting Avg', 0):.0%} | "
                f"{m.get('Upside / Downside', 0):.2f} | {m.get('Upside Capture', 0):.0%} | {m.get('Downside Capture', 0):.0%} |"
            )

        # Add Rolling Profiles
        report.extend(["\n## 🧬 Performance Consistency (Rolling Profiles)", "| Window | Median | Max | Min | Outperformance % |", "| :--- | :--- | :--- | :--- | :--- |"])
        for label, stats in rolling_profiles.items():
            if isinstance(stats, dict):
                report.append(f"| {label} | {stats.get('Median Return', 0):.1%} | {stats.get('Maximum Return', 0):.1%} | {stats.get('Minimum Return', 0):.1%} | {stats.get('Outperformance', 0):.0%} |")

        # Add Stress Scenarios
        report.extend(["\n## 🛡️ Historical Resilience (Market Stress Scenarios)", "| Crisis Event | Fund Performance | Benchmark | Capture Ratio |", "| :--- | :--- | :--- | :--- |"])
        if not stress_df.empty:
            for _, row in stress_df.iterrows():
                cap_val = row.get("Capture Ratio", "-")
                cap_str = f"{cap_val:.2f}" if isinstance(cap_val, (int, float)) else "-"
                report.append(f"| {row['Crisis']} | {row['Fund Drop']:.1%} | {row['Benchmark Drop']:.1%} | {cap_str} |")
        else:
            report.append("| No history for major crises | - | - | - |")

        report.extend(
            [
                "\n# CONSTRAINTS & RIGOR",
                "* **Tone:** Professional, objective, and technical.",
                "* **Evidence-Based:** Supporting claims with provided data points.",
                "* **Risk Focus:** Focus on Portfolio Characteristics, Liquidity, and Fiduciary Suitability.",
                "\n# OUTPUT STRUCTURE",
                "1. **Executive Summary:** A 2-sentence high-level verdict.",
                "2. **Quantitative Breakdown:** Sub-sections on Defensive Fortitude and Structural Consistency.",
                "3. **Tactical Actionables:** Exactly 3 high-impact recommendations.",
                "\n---",
                "**Analyst Task:** Decipher the data above and generate the Performance Evaluation Report.",
            ]
        )

        return "\n".join(report)
