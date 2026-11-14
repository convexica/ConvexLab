from unittest.mock import MagicMock, patch

import pytest

from app.core.data_fetcher import MFDataFetcher


def test_sync_from_cloud_cache_path_traversal():
    fetcher = MFDataFetcher()
    # Directory traversal attempts must be rejected
    assert fetcher._sync_from_cloud_cache("../traversal.csv") is False
    assert fetcher._sync_from_cloud_cache("dir/traversal.csv") is False
    assert fetcher._sync_from_cloud_cache("") is False


def test_get_cache_path_traversal_rejected():
    fetcher = MFDataFetcher()
    # Non-digit or directory traversal attempts must raise ValueError
    with pytest.raises(ValueError):
        fetcher._get_cache_path("../traversal")
    with pytest.raises(ValueError):
        fetcher._get_cache_path("dir/scheme")
    with pytest.raises(ValueError):
        fetcher._get_cache_path("abc123")
    with pytest.raises(ValueError):
        fetcher._get_cache_path("")


def test_get_cache_path_valid():
    fetcher = MFDataFetcher()
    path = fetcher._get_cache_path("122639")
    assert path.name == "122639.csv"
    assert path == fetcher.cache_dir / "122639.csv"


def test_sync_from_cloud_cache_mocked_success(tmp_path):
    fetcher = MFDataFetcher()
    fetcher.cache_dir = tmp_path

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"date,nav\n2026-01-01,100.0\n"

    with patch.object(fetcher.session, "get", return_value=mock_response):
        success = fetcher._sync_from_cloud_cache("test_fund.csv")
        assert success is True
        target_file = tmp_path / "test_fund.csv"
        assert target_file.exists()
        assert target_file.read_bytes() == b"date,nav\n2026-01-01,100.0\n"


def test_get_fund_info_traversal_rejected():
    fetcher = MFDataFetcher()
    with pytest.raises(ValueError):
        fetcher.get_fund_info("../traversal")
    with pytest.raises(ValueError):
        fetcher.get_fund_info("dir/scheme")
    with pytest.raises(ValueError):
        fetcher.get_fund_info("abc123")
    with pytest.raises(ValueError):
        fetcher.get_fund_info("")


def test_get_current_risk_free_rate_anomalous_discarded():
    fetcher = MFDataFetcher()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"Value": 99.99}'

    with patch.object(fetcher.session, "get", return_value=mock_response):
        rate = fetcher.get_current_risk_free_rate()
        # Anomalous rate > 25% must be discarded, falling back to 0.065
        assert rate == 0.065
