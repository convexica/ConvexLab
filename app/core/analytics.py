from typing import Any, Dict, Optional, Tuple, Union

import numpy as np
import pandas as pd
import streamlit as st


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

        returns = nav_series.pct_change().dropna()
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

        returns = nav_series.pct_change().dropna()
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
        vals = nav_series.values

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
        if hasattr(benchmark_nav, "squeeze"):
            benchmark_nav = benchmark_nav.squeeze()

        df = pd.DataFrame({"fund": fund_nav, "bench": benchmark_nav}).dropna()
        if df.empty:
            return {"upside": 0.0, "downside": 0.0}

        # Monthly resampled returns
        monthly_df = df.resample("ME").last().pct_change().dropna()

        upside_bench = monthly_df[monthly_df["bench"] > 0]
        downside_bench = monthly_df[monthly_df["bench"] <= 0]

        upside_ratio = (upside_bench["fund"].mean() / upside_bench["bench"].mean()) if not upside_bench.empty else 0
        downside_ratio = (downside_bench["fund"].mean() / downside_bench["bench"].mean()) if not downside_bench.empty else 0

        return {"upside": upside_ratio * 100, "downside": downside_ratio * 100}

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
        if hasattr(benchmark_nav, "squeeze"):
            benchmark_nav = benchmark_nav.squeeze()

        df = pd.DataFrame({"fund": fund_nav, "bench": benchmark_nav}).dropna()
        if len(df) < 20:
            return {"alpha": 0.0, "beta": 0.0, "r_squared": 0.0}

        f_ret = df["fund"].pct_change().dropna()
        b_ret = df["bench"].pct_change().dropna()

        daily_rf = rf / 252
        f_excess = f_ret - daily_rf
        b_excess = b_ret - daily_rf

        # CAPM Regression
        beta, alpha_daily = np.polyfit(b_excess, f_excess, 1)
        alpha_annual = alpha_daily * 252

        # Stats
        correlation = np.corrcoef(b_excess, f_excess)[0, 1]
        r_squared = correlation**2

        # Information Ratio
        active_returns = f_ret - b_ret
        tracking_error = active_returns.std() * np.sqrt(252)
        info_ratio = (active_returns.mean() * 252) / tracking_error if tracking_error != 0 else 0

        # Batting Average
        batting_avg = (f_ret > b_ret).mean() * 100

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
        returns = yearly_nav.pct_change()

        # Handle first year (possibly partial)
        if not yearly_nav.empty:
            returns.iloc[0] = (yearly_nav.iloc[0] / nav_series.iloc[0]) - 1

        returns.index = returns.index.year
        return returns

    @st.cache_data(show_spinner=False)
    def calculate_rolling_return_profile(_self, nav_series: pd.Series) -> Dict[str, Any]:
        """Generate statistical profiles for standard rolling horizons."""
        profile = {}
        horizons = {1: "1 Year", 3: "3 Years", 5: "5 Years"}

        for yrs, label in horizons.items():
            rolling = _self.calculate_rolling_returns(nav_series, window_years=yrs)
            if rolling.empty:
                profile[label] = None
                continue

            profile[label] = {
                "Minimum": rolling.min(),
                "Median": rolling.median(),
                "Maximum": rolling.max(),
                "% times -ve returns": (rolling < 0).mean(),
                "% times returns 0 - 5%": ((rolling >= 0.00) & (rolling < 0.05)).mean(),
                "% times returns 5 - 10%": ((rolling >= 0.05) & (rolling < 0.10)).mean(),
                "% times returns 10 - 15%": ((rolling >= 0.10) & (rolling < 0.15)).mean(),
                "% times returns 15 - 20%": ((rolling >= 0.15) & (rolling < 0.20)).mean(),
                "% times returns > 20%": (rolling >= 0.20).mean(),
            }
        return profile
