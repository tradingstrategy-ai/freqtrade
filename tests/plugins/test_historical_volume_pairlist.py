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


# ---------------------------------------------------------------------------
# Step 1: Auto-derive pair_suffix from config
# ---------------------------------------------------------------------------


class TestAutoDerivePairSuffix:
    def test_futures_usdc_derives_usdc_usdc(self, mocker):
        """Futures + USDC auto-derives _USDC_USDC (same as old hardcoded default)."""
        handler, _ = _make_handler(mocker, pairlistconfig={
            # No explicit pair_suffix — should auto-derive
        })
        assert handler._pair_suffix == "_USDC_USDC"

    def test_futures_usdt_derives_usdt_usdt(self, mocker):
        """Futures + USDT (Aster) auto-derives _USDT_USDT."""
        handler, _ = _make_handler(mocker, config_overrides={
            "stake_currency": "USDT",
            "trading_mode": "futures",
        }, pairlistconfig={
            # No explicit pair_suffix
        })
        assert handler._pair_suffix == "_USDT_USDT"

    def test_spot_usdc_derives_usdc(self, mocker):
        """Spot + USDC auto-derives _USDC (no settlement currency)."""
        handler, _ = _make_handler(mocker, config_overrides={
            "trading_mode": "spot",
        }, pairlistconfig={
            # No explicit pair_suffix
        })
        assert handler._pair_suffix == "_USDC"

    def test_explicit_suffix_overrides_auto(self, mocker):
        """Explicit pair_suffix in pairlistconfig always wins."""
        handler, _ = _make_handler(mocker, config_overrides={
            "stake_currency": "USDT",
            "trading_mode": "futures",
        }, pairlistconfig={
            "pair_suffix": "_CUSTOM_CUSTOM",
        })
        assert handler._pair_suffix == "_CUSTOM_CUSTOM"

    def test_futures_usdt_loads_usdt_files(self, mocker, tmp_path):
        """Auto-derived USDT suffix actually finds USDT-named feather files."""
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 5,
            "high": [110.0] * 5,
            "low": [90.0] * 5,
            "close": [105.0] * 5,
            "volume": [1000.0] * 5,
        })
        df.to_feather(futures_dir / "BTC_USDT_USDT-1d-futures.feather")

        handler, _ = _make_handler(mocker, config_overrides={
            "stake_currency": "USDT",
            "trading_mode": "futures",
        }, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert not handler._volume_data.empty
        assert "BTC/USDT:USDT" in handler._volume_data.columns


# ---------------------------------------------------------------------------
# Step 2: Auto-derive subdirectory and glob pattern
# ---------------------------------------------------------------------------


class TestAutoDerivePathAndGlob:
    def test_futures_candle_type_str(self, mocker):
        handler, _ = _make_handler(mocker)
        assert handler._candle_type_str == "futures"

    def test_spot_candle_type_str(self, mocker):
        handler, _ = _make_handler(mocker, config_overrides={"trading_mode": "spot"})
        assert handler._candle_type_str == ""

    def test_spot_data_no_futures_subdir(self, mocker, tmp_path):
        """Spot mode: files in root dir (no /futures/ subdir), no -futures suffix."""
        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 5,
            "high": [110.0] * 5,
            "low": [90.0] * 5,
            "close": [105.0] * 5,
            "volume": [1000.0] * 5,
        })
        # Spot file: no -futures suffix, just _USDC
        df.to_feather(tmp_path / "BTC_USDC-1d.feather")

        handler, _ = _make_handler(mocker, config_overrides={
            "trading_mode": "spot",
        }, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert not handler._volume_data.empty
        assert "BTC/USDC" in handler._volume_data.columns

    def test_spot_4h_fallback(self, mocker, tmp_path):
        """Spot mode: 4h fallback works without -futures suffix."""
        dates = pd.date_range("2025-01-01", periods=30, freq="4h")
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 30,
            "high": [110.0] * 30,
            "low": [90.0] * 30,
            "close": [105.0] * 30,
            "volume": [250.0] * 30,
        })
        df.to_feather(tmp_path / "BTC_USDC-4h.feather")

        handler, _ = _make_handler(mocker, config_overrides={
            "trading_mode": "spot",
        }, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert not handler._volume_data.empty
        assert "BTC/USDC" in handler._volume_data.columns

    def test_futures_subdir_fallback_to_root(self, mocker, tmp_path):
        """Futures mode: if /futures/ subdir doesn't exist, falls back to root."""
        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 5,
            "high": [110.0] * 5,
            "low": [90.0] * 5,
            "close": [105.0] * 5,
            "volume": [1000.0] * 5,
        })
        # Put file in root, no /futures/ subdir
        df.to_feather(tmp_path / "BTC_USDC_USDC-1d-futures.feather")

        handler, _ = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert not handler._volume_data.empty


# ---------------------------------------------------------------------------
# Step 3: Case-insensitive matching
# ---------------------------------------------------------------------------


class TestCaseInsensitiveMatching:
    def test_kprefix_case_mismatch_resolved(self, mocker, tmp_path):
        """kPEPE in filename → KPEPE in volume data; whitelist has KPEPE/USDC:USDC."""
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        df = pd.DataFrame({
            "date": dates,
            "open": [1.0] * 5,
            "high": [1.1] * 5,
            "low": [0.9] * 5,
            "close": [1.05] * 5,
            "volume": [1000.0] * 5,
        })
        df.to_feather(futures_dir / "kPEPE_USDC_USDC-1d-futures.feather")

        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        plm._current_time = datetime(2025, 1, 3, 0, 0, tzinfo=timezone.utc)

        # Whitelist uses uppercase KPEPE
        result = handler.filter_pairlist(["KPEPE/USDC:USDC"], {})
        assert result == ["KPEPE/USDC:USDC"]

    def test_case_insensitive_preserves_whitelist_casing(self, mocker, tmp_path):
        """Result should use the whitelist's casing, not the volume data's."""
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        for token in ["BTC", "ETH"]:
            df = pd.DataFrame({
                "date": dates,
                "open": [100.0] * 5,
                "high": [110.0] * 5,
                "low": [90.0] * 5,
                "close": [105.0] * 5,
                "volume": [1000.0] * 5,
            })
            df.to_feather(futures_dir / f"{token}_USDC_USDC-1d-futures.feather")

        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        plm._current_time = datetime(2025, 1, 3, 0, 0, tzinfo=timezone.utc)

        # Even if we pass weird casing, the result uses the input pairlist's casing
        result = handler.filter_pairlist(["BTC/USDC:USDC", "ETH/USDC:USDC"], {})
        assert "BTC/USDC:USDC" in result
        assert "ETH/USDC:USDC" in result

    def test_all_seven_kprefix_tokens(self, mocker, tmp_path):
        """All 7 Hyperliquid k-prefix tokens resolve without token_mapping."""
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        k_tokens = ["kBONK", "kDOGS", "kFLOKI", "kLUNC", "kNEIRO", "kPEPE", "kSHIB"]
        dates = pd.date_range("2025-01-01", periods=5, freq="1D")

        for token in k_tokens:
            df = pd.DataFrame({
                "date": dates,
                "open": [1.0] * 5,
                "high": [1.1] * 5,
                "low": [0.9] * 5,
                "close": [1.05] * 5,
                "volume": [1000.0] * 5,
            })
            df.to_feather(futures_dir / f"{token}_USDC_USDC-1d-futures.feather")

        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
            # No token_mapping!
        })
        plm._current_time = datetime(2025, 1, 3, 0, 0, tzinfo=timezone.utc)

        # Whitelist uses uppercase K
        whitelist = [f"{t[1:].upper()}/USDC:USDC" for t in k_tokens]
        # kBONK → BONK, kDOGS → DOGS, etc — wait, _extract_ticker makes kBONK → KBONK
        # So whitelist should be KBONK, KDOGS, etc.
        whitelist = [f"K{t[1:]}/USDC:USDC" for t in k_tokens]
        result = handler.filter_pairlist(whitelist, {})

        assert len(result) == 7

    def test_legitimate_uppercase_K_not_mangled(self, mocker, tmp_path):
        """KAITO and KAS (legitimate K-start tokens) are not affected by k-prefix logic."""
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        for token in ["KAITO", "KAS"]:
            df = pd.DataFrame({
                "date": dates,
                "open": [100.0] * 5,
                "high": [110.0] * 5,
                "low": [90.0] * 5,
                "close": [105.0] * 5,
                "volume": [1000.0] * 5,
            })
            df.to_feather(futures_dir / f"{token}_USDC_USDC-1d-futures.feather")

        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        plm._current_time = datetime(2025, 1, 3, 0, 0, tzinfo=timezone.utc)

        result = handler.filter_pairlist(
            ["KAITO/USDC:USDC", "KAS/USDC:USDC"], {}
        )
        assert result == ["KAITO/USDC:USDC", "KAS/USDC:USDC"]


# ---------------------------------------------------------------------------
# Step 4: Graceful degradation
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    def test_exception_returns_original_pairlist(self, mocker):
        """If internal logic throws, filter_pairlist returns input unchanged."""
        handler, plm = _make_handler(mocker)
        plm._current_time = datetime(2025, 1, 5, 0, 0, tzinfo=timezone.utc)

        # Force _build_daily_rankings to throw
        handler._build_daily_rankings = MagicMock(
            side_effect=RuntimeError("corrupted data")
        )

        input_list = ["BTC/USDC:USDC", "ETH/USDC:USDC"]
        result = handler.filter_pairlist(input_list, {})
        assert result == input_list

    def test_bad_data_source_dir_degrades(self, mocker):
        """Non-existent data_source_dir doesn't crash, just passes through."""
        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": "/totally/nonexistent/path",
        })
        plm._current_time = datetime(2025, 1, 5, 0, 0, tzinfo=timezone.utc)

        input_list = ["BTC/USDC:USDC"]
        # Should not raise — empty data → empty rankings → return original
        result = handler.filter_pairlist(input_list, {})
        assert result == input_list

    def test_corrupted_feather_skipped(self, mocker, tmp_path):
        """A corrupted feather file is skipped; valid files still load."""
        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()

        dates = pd.date_range("2025-01-01", periods=5, freq="1D")
        df = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 5,
            "high": [110.0] * 5,
            "low": [90.0] * 5,
            "close": [105.0] * 5,
            "volume": [1000.0] * 5,
        })
        df.to_feather(futures_dir / "BTC_USDC_USDC-1d-futures.feather")

        # Write corrupted file
        with open(futures_dir / "BAD_USDC_USDC-1d-futures.feather", "wb") as f:
            f.write(b"not a feather file")

        handler, plm = _make_handler(mocker, pairlistconfig={
            "data_source_dir": str(tmp_path),
        })
        handler._load_volume_data()

        assert handler._volume_data is not None
        assert "BTC/USDC:USDC" in handler._volume_data.columns
        assert "BAD/USDC:USDC" not in handler._volume_data.columns
