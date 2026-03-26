import numpy as np
import pandas as pd
import pytest
from core.analytics import MFAnalytics


@pytest.fixture
def analytics():
    # Use 0% risk free for simple tests
    return MFAnalytics(risk_free_rate=0.0)


def test_calculate_cagr(analytics):
    # Create exactly 1 year of data
    dates = pd.to_datetime(["2023-01-01", "2024-01-01"])
    nav = pd.Series([100.0, 110.0], index=dates)

    cagr = analytics.calculate_cagr(nav)
    # (110/100)^(1/0.999) ...
    # (2024-01-01 - 2023-01-01).days / 365.25 = 365 / 365.25 = 0.9993
    # Let's just test if it's approximately correct
    assert pytest.approx(cagr, rel=1e-2) == 0.10


def test_calculate_drawdowns(analytics):
    dates = pd.date_range("2023-01-01", periods=5, freq="D")
    nav = pd.Series([100, 110, 99, 105, 120], index=dates)
    # High was 110, dropped to 99 -> (99-110)/110 = -0.10
    _, max_dd = analytics.calculate_drawdowns(nav)
    assert pytest.approx(max_dd) == -0.10


def test_calculate_risk_metrics(analytics):
    # 2 years of daily data with 10% annual return and 15% volatility
    np.random.seed(42)
    dates = pd.date_range("2021-01-01", periods=504, freq="B")
    returns = np.random.normal(0.10 / 252, 0.15 / np.sqrt(252), 504)
    nav = 100 * (1 + returns).cumprod()
    nav_series = pd.Series(nav, index=dates)

    metrics = analytics.calculate_risk_metrics(nav_series)
    assert "sharpe_ratio" in metrics
    assert "volatility" in metrics
    # Volatility should be around 15%
    assert pytest.approx(metrics["volatility"], rel=0.2) == 0.15


def test_empty_series_handling(analytics):
    empty_nav = pd.Series(dtype=float)
    assert analytics.calculate_cagr(empty_nav) == 0.0
    metrics = analytics.calculate_risk_metrics(empty_nav)
    assert metrics == {}
