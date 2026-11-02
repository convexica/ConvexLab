from unittest.mock import MagicMock, patch

from app.core.data_fetcher import MFDataFetcher


def test_sync_from_cloud_cache_path_traversal():
    fetcher = MFDataFetcher()
    # Directory traversal attempts must be rejected
    assert fetcher._sync_from_cloud_cache("../traversal.csv") is False
    assert fetcher._sync_from_cloud_cache("dir/traversal.csv") is False
    assert fetcher._sync_from_cloud_cache("") is False


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
