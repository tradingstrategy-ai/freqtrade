"""
Tests for HistoricalVolumePairList pairlist plugin.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, PropertyMock

import pandas as pd
import pytest

from freqtrade.plugins.pairlist.HistoricalVolumePairList import HistoricalVolumePairList
from freqtrade.plugins.pairlist.IPairList import SupportsBacktesting
from freqtrade.plugins.pairlistmanager import PairListManager
from tests.conftest import EXMS, get_patched_exchange


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def volume_data_dir(tmp_path):
    """Create temporary 1d feather files with known volumes for 3 tokens."""
    futures_dir = tmp_path / "futures"
    futures_dir.mkdir()

    dates = pd.date_range("2025-01-01", periods=10, freq="1D")
    tokens = [
        ("BTC", 1000.0),   # highest volume
        ("ETH", 500.0),    # medium volume
        ("DOGE", 50.0),    # lowest volume
    ]

    for token, vol in tokens:
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 10,
            "high": [110.0] * 10,
            "low": [90.0] * 10,
            "close": [105.0] * 10,
            "volume": [vol] * 10,
        })
        df.to_feather(futures_dir / f"{token}_USDC_USDC-1d-futures.feather")

    return tmp_path


@pytest.fixture
def volume_data_dir_4h(tmp_path):
    """Create temporary 4h feather files (no 1d) to test fallback."""
    futures_dir = tmp_path / "futures"
    futures_dir.mkdir()

    dates = pd.date_range("2025-01-01", periods=60, freq="4h")  # 10 days of 4h candles
    tokens = [("BTC", 250.0), ("ETH", 125.0)]  # per-4h volumes

    for token, vol in tokens:
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 60,
            "high": [110.0] * 60,
            "low": [90.0] * 60,
            "close": [105.0] * 60,
            "volume": [vol] * 60,
        })
        df.to_feather(futures_dir / f"{token}_USDC_USDC-4h-futures.feather")

    return tmp_path


def _make_handler(mocker, config_overrides=None, pairlistconfig=None):
    """Helper to instantiate HistoricalVolumePairList with minimal mocking."""
    config = {
        "stake_currency": "USDC",
        "trading_mode": "futures",
        "exchange": {"name": "hyperliquid", "pair_whitelist": []},
        "pairlists": [{"method": "StaticPairList"}],
    }
    if config_overrides:
        config.update(config_overrides)

    plconfig = {
        "method": "HistoricalVolumePairList",
        "data_source_dir": "/nonexistent",
        "number_assets": 75,
        "lookback_days": 7,
        "min_value": 0,
        "pair_suffix": "_USDC_USDC",
    }
    if pairlistconfig:
        plconfig.update(pairlistconfig)

    exchange = MagicMock()
    exchange.get_markets.return_value = {}
    plm = MagicMock()
    plm._current_time = None

    handler = HistoricalVolumePairList(
        exchange=exchange,
        pairlistmanager=plm,
        config=config,
        pairlistconfig=plconfig,
        pairlist_pos=1,
    )
    return handler, plm


# ---------------------------------------------------------------------------
# Unit tests: ticker extraction & pair formatting
# ---------------------------------------------------------------------------


class TestTickerExtraction:
    def test_basic_ticker(self, mocker):
        handler, _ = _make_handler(mocker)
        assert handler._extract_ticker("BTC_USDC_USDC-1d-futures.feather") == "BTC"
        assert handler._extract_ticker("ETH_USDC_USDC-1d-futures.feather") == "ETH"

    def test_k_prefix_normalization(self, mocker):
        handler, _ = _make_handler(mocker)
        assert handler._extract_ticker("kPEPE_USDC_USDC-1d-futures.feather") == "KPEPE"
        assert handler._extract_ticker("kSHIB_USDC_USDC-1d-futures.feather") == "KSHIB"

    def test_k_prefix_no_false_positive(self, mocker):
        """Lowercase k followed by lowercase should not be normalized."""
        handler, _ = _make_handler(mocker)
        assert handler._extract_ticker("kaspa_USDC_USDC-1d-futures.feather") == "kaspa"

    def test_token_mapping(self, mocker):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "token_mapping": {"1000BONK": "BONK", "1000PEPE": "PEPE"},
        })
        assert handler._extract_ticker("1000BONK_USDC_USDC-1d-futures.feather") == "BONK"
        assert handler._extract_ticker("1000PEPE_USDC_USDC-1d-futures.feather") == "PEPE"

    def test_token_mapping_takes_precedence_over_k_prefix(self, mocker):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "token_mapping": {"kPEPE": "CUSTOMPEPE"},
        })
        assert handler._extract_ticker("kPEPE_USDC_USDC-1d-futures.feather") == "CUSTOMPEPE"

    def test_usdt_suffix(self, mocker):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "pair_suffix": "_USDT_USDT",
        })
        assert handler._extract_ticker("BTC_USDT_USDT-1d-futures.feather") == "BTC"


class TestTickerToPair:
    def test_futures_pair(self, mocker):
        handler, _ = _make_handler(mocker)
        assert handler._ticker_to_pair("BTC") == "BTC/USDC:USDC"

    def test_futures_pair_usdt(self, mocker):
        handler, _ = _make_handler(mocker, config_overrides={"stake_currency": "USDT"})
        assert handler._ticker_to_pair("BTC") == "BTC/USDT:USDT"

    def test_spot_pair(self, mocker):
        handler, _ = _make_handler(mocker, config_overrides={"trading_mode": "spot"})
        assert handler._ticker_to_pair("BTC") == "BTC/USDC"


# ---------------------------------------------------------------------------
# Unit tests: data loading
# ---------------------------------------------------------------------------


class TestDataLoading:
    def test_load_volume_data_1d(self, mocker, volume_data_dir):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert not handler._volume_data.empty
        assert set(handler._volume_data.columns) == {
            "BTC/USDC:USDC", "ETH/USDC:USDC", "DOGE/USDC:USDC"
        }
        assert len(handler._volume_data) == 10

        # Verify quoteVolume = volume * typical_price
        # typical_price = (110 + 90 + 105) / 3 = 101.6667
        expected_btc = 1000.0 * (110.0 + 90.0 + 105.0) / 3
        assert abs(handler._volume_data["BTC/USDC:USDC"].iloc[0] - expected_btc) < 0.01

    def test_load_volume_data_4h_fallback(self, mocker, volume_data_dir_4h):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir_4h),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert not handler._volume_data.empty
        # 4h data resampled to daily: 60 candles / 6 per day = 10 days
        assert len(handler._volume_data) == 10
        assert "BTC/USDC:USDC" in handler._volume_data.columns

    def test_load_volume_data_no_files(self, mocker, tmp_path):
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert handler._volume_data.empty


# ---------------------------------------------------------------------------
# Unit tests: ranking
# ---------------------------------------------------------------------------


class TestDailyRankings:
    def test_build_rankings_order(self, mocker, volume_data_dir):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
            "number_assets": 10,
            "lookback_days": 3,
        })
        handler._build_daily_rankings()

        assert handler._daily_rankings is not None
        assert len(handler._daily_rankings) == 10

        # Check that rankings are sorted by volume (BTC > ETH > DOGE)
        day = "2025-01-05"
        ranked = handler._daily_rankings[day]
        assert ranked[0] == "BTC/USDC:USDC"
        assert ranked[1] == "ETH/USDC:USDC"
        assert ranked[2] == "DOGE/USDC:USDC"

    def test_build_rankings_top_n(self, mocker, volume_data_dir):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
            "number_assets": 2,
        })
        handler._build_daily_rankings()

        day = "2025-01-05"
        ranked = handler._daily_rankings[day]
        assert len(ranked) == 2
        assert "DOGE/USDC:USDC" not in ranked

    def test_build_rankings_min_value(self, mocker, volume_data_dir):
        # typical_price = 101.6667, DOGE volume = 50 → quoteVol per day ~ 5083
        # 7-day rolling sum for DOGE ~ 35583
        # ETH volume = 500 → quoteVol per day ~ 50833, 7-day ~ 355833
        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
            "min_value": 100000,
        })
        handler._build_daily_rankings()

        # After enough days to build a window, DOGE should be filtered out
        day = "2025-01-10"
        ranked = handler._daily_rankings[day]
        assert "DOGE/USDC:USDC" not in ranked
        assert "BTC/USDC:USDC" in ranked
        assert "ETH/USDC:USDC" in ranked

    def test_build_rankings_empty_data(self, mocker, tmp_path):
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._build_daily_rankings()

        assert handler._daily_rankings == {}


# ---------------------------------------------------------------------------
# Unit tests: filter_pairlist
# ---------------------------------------------------------------------------


class TestFilterPairlist:
    def test_filter_intersection_and_sort(self, mocker, volume_data_dir):
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
            "number_assets": 10,
        })
        plm._current_time = datetime(2025, 1, 5, 12, 0, tzinfo=timezone.utc)

        # Input in arbitrary order, including a pair not in volume data
        input_list = ["DOGE/USDC:USDC", "BTC/USDC:USDC", "UNKNOWN/USDC:USDC", "ETH/USDC:USDC"]
        result = handler.filter_pairlist(input_list, {})

        # Should be sorted by volume (BTC > ETH > DOGE), UNKNOWN removed
        assert result == ["BTC/USDC:USDC", "ETH/USDC:USDC", "DOGE/USDC:USDC"]

    def test_filter_closest_date_fallback(self, mocker, volume_data_dir):
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
        })
        # Date after data range — should fall back to last available date
        plm._current_time = datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc)

        result = handler.filter_pairlist(["BTC/USDC:USDC", "ETH/USDC:USDC"], {})
        assert len(result) == 2
        assert result[0] == "BTC/USDC:USDC"

    def test_filter_no_current_time_uses_utcnow(self, mocker, volume_data_dir):
        """When _current_time is None, falls back to datetime.now(UTC)."""
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
        })
        plm._current_time = None

        # Should not crash — will use current date and fall back to closest
        result = handler.filter_pairlist(["BTC/USDC:USDC"], {})
        # Data is from 2025-01-01 to 2025-01-10, current date is 2026+
        # Should fall back to last available date
        assert result == ["BTC/USDC:USDC"]

    def test_filter_no_rankings_returns_original(self, mocker, tmp_path):
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        plm._current_time = datetime(2025, 1, 5, 0, 0, tzinfo=timezone.utc)

        input_list = ["BTC/USDC:USDC", "ETH/USDC:USDC"]
        # Empty data → empty rankings → no earlier date found → return original
        result = handler.filter_pairlist(input_list, {})
        assert result == input_list

    def test_filter_date_before_data_range(self, mocker, volume_data_dir):
        """Date before any data should return original pairlist."""
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
        })
        plm._current_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

        input_list = ["BTC/USDC:USDC"]
        result = handler.filter_pairlist(input_list, {})
        assert result == input_list


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


class TestConfiguration:
    def test_missing_data_source_dir_raises(self, mocker):
        with pytest.raises(ValueError, match="data_source_dir"):
            _make_handler(mocker, pairlistconfig={"data_source_dir": ""})

    def test_supports_backtesting(self, mocker):
        handler, _ = _make_handler(mocker)
        assert handler.supports_backtesting == SupportsBacktesting.YES

    def test_needstickers_false(self, mocker):
        handler, _ = _make_handler(mocker)
        assert handler.needstickers is False

    def test_short_desc(self, mocker):
        handler, _ = _make_handler(mocker, pairlistconfig={
            "number_assets": 50,
            "lookback_days": 14,
            "min_value": 100000,
        })
        desc = handler.short_desc()
        assert "Top 50" in desc
        assert "lookback=14d" in desc
        assert "100000" in desc

    def test_description(self):
        assert "volume" in HistoricalVolumePairList.description().lower()

    def test_available_parameters(self):
        params = HistoricalVolumePairList.available_parameters()
        assert "data_source_dir" in params
        assert "number_assets" in params
        assert "lookback_days" in params
        assert "min_value" in params
        assert "pair_suffix" in params
        assert "token_mapping" in params


# ---------------------------------------------------------------------------
# Integration: pairlist chain via PairListManager
# ---------------------------------------------------------------------------


class TestPairlistChainIntegration:
    def test_static_plus_historical_volume(self, mocker, volume_data_dir):
        """StaticPairList → HistoricalVolumePairList filters correctly in a chain."""
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
            "number_assets": 2,
            "lookback_days": 7,
        })
        plm._current_time = datetime(2025, 1, 8, 0, 0, tzinfo=timezone.utc)

        # Simulate StaticPairList output (all 3 pairs)
        static_output = ["BTC/USDC:USDC", "ETH/USDC:USDC", "DOGE/USDC:USDC"]
        result = handler.filter_pairlist(static_output, {})

        # Top 2 by volume: BTC and ETH (DOGE filtered out)
        assert len(result) == 2
        assert result[0] == "BTC/USDC:USDC"
        assert result[1] == "ETH/USDC:USDC"
        assert "DOGE/USDC:USDC" not in result

    def test_chain_preserves_volume_order_not_input_order(self, mocker, volume_data_dir):
        """Verify output is sorted by volume, not by input order."""
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(volume_data_dir),
            "number_assets": 10,
        })
        plm._current_time = datetime(2025, 1, 8, 0, 0, tzinfo=timezone.utc)

        # Input in reverse volume order
        result = handler.filter_pairlist(
            ["DOGE/USDC:USDC", "ETH/USDC:USDC", "BTC/USDC:USDC"], {}
        )
        # Output should be BTC > ETH > DOGE regardless of input order
        assert result == ["BTC/USDC:USDC", "ETH/USDC:USDC", "DOGE/USDC:USDC"]
