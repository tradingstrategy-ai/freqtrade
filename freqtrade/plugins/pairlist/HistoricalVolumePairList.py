"""
Historical Volume PairList - Backtestable volume sorting from local feather files.

Reads historical daily candle feather files from a data directory and sorts pairs
by rolling quoteVolume. Supports backtesting with time-aware daily rankings.

Works with any exchange's own data or cross-venue data (e.g. Binance volume for GMX).

Usage in config (as a filter after StaticPairList):
    "pairlists": [
        {"method": "StaticPairList"},
        {
            "method": "HistoricalVolumePairList",
            "data_source_dir": "user_data/data/hyperliquid",
            "number_assets": 75,
            "lookback_days": 7,
            "min_value": 100000,
            "pair_suffix": "_USDC_USDC",
            "token_mapping": {"kPEPE": "KPEPE"}
        }
    ],
    "enable_dynamic_pairlist": true
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from freqtrade.exchange.exchange_types import Tickers
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter, SupportsBacktesting


logger = logging.getLogger(__name__)


class HistoricalVolumePairList(IPairList):
    """
    Sort pairs by historical quoteVolume from local feather files.

    Receives a pairlist (e.g. from StaticPairList), looks up each pair's volume
    from the data source, and returns the top N pairs sorted by rolling average
    quoteVolume. Time-aware during backtesting via pairlist manager's _current_time.
    """

    supports_backtesting = SupportsBacktesting.YES

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._data_source_dir: str = self._pairlistconfig.get("data_source_dir", "")
        if not self._data_source_dir:
            raise ValueError(
                "HistoricalVolumePairList requires 'data_source_dir' in config "
                "(e.g. 'user_data/data/hyperliquid')"
            )

        self._number_assets: int = self._pairlistconfig.get("number_assets", 75)
        self._lookback_days: int = self._pairlistconfig.get("lookback_days", 7)
        self._min_value: float = self._pairlistconfig.get("min_value", 0)
        self._pair_suffix: str = self._pairlistconfig.get("pair_suffix", "_USDC_USDC")
        self._token_mapping: dict[str, str] = self._pairlistconfig.get("token_mapping", {})

        # Derive quote currency from config
        self._stake_currency: str = self._config.get("stake_currency", "USDC")

        # Lazy-loaded data
        self._volume_data: pd.DataFrame | None = None
        self._daily_rankings: dict[str, list[str]] | None = None

    @property
    def needstickers(self) -> bool:
        return False

    def short_desc(self) -> str:
        return (
            f"{self.name} - Top {self._number_assets} by volume "
            f"(lookback={self._lookback_days}d, min={self._min_value}, "
            f"source={self._data_source_dir})"
        )

    @staticmethod
    def description() -> str:
        return "Sort pairs by historical volume from local feather files."

    @staticmethod
    def available_parameters() -> dict[str, PairlistParameter]:
        return {
            "data_source_dir": {
                "type": "string",
                "default": "",
                "description": "Data source directory",
                "help": "Path to the data directory (e.g. user_data/data/hyperliquid).",
            },
            "number_assets": {
                "type": "number",
                "default": 75,
                "description": "Number of assets",
                "help": "Maximum number of pairs to return, sorted by volume.",
            },
            "lookback_days": {
                "type": "number",
                "default": 7,
                "description": "Lookback days",
                "help": "Number of days for rolling volume calculation.",
            },
            "min_value": {
                "type": "number",
                "default": 0,
                "description": "Minimum quoteVolume",
                "help": "Minimum rolling quoteVolume to include a pair.",
            },
            "pair_suffix": {
                "type": "string",
                "default": "_USDC_USDC",
                "description": "Source file pair suffix",
                "help": "File naming suffix (e.g. '_USDC_USDC' or '_USDT_USDT').",
            },
            "token_mapping": {
                "type": "object",
                "default": {},
                "description": "Token name mapping",
                "help": "Map source file token names to trading pair names (e.g. {'kPEPE': 'KPEPE'}).",
            },
        }

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _extract_ticker(self, filename: str) -> str:
        """Extract and normalize ticker from a feather filename.

        e.g. 'BTC_USDC_USDC-1d-futures.feather' -> 'BTC'
             'kPEPE_USDC_USDC-1d-futures.feather' -> 'KPEPE' (with k-prefix normalization)
        """
        # Strip the suffix pattern to get the ticker
        ticker = filename.split(f"{self._pair_suffix}-")[0]

        # Apply explicit token mapping first
        if ticker in self._token_mapping:
            return self._token_mapping[ticker]

        # Normalize k-prefix tickers: kPEPE -> KPEPE (Hyperliquid convention)
        if ticker.startswith("k") and len(ticker) > 1 and ticker[1].isupper():
            ticker = "K" + ticker[1:]

        return ticker

    def _ticker_to_pair(self, ticker: str) -> str:
        """Convert ticker to freqtrade pair format.

        e.g. 'BTC' -> 'BTC/USDC:USDC' (futures)
        """
        sc = self._stake_currency
        trading_mode = self._config.get("trading_mode", "spot")
        if trading_mode == "futures":
            return f"{ticker}/{sc}:{sc}"
        return f"{ticker}/{sc}"

    def _load_volume_data(self) -> None:
        """Load daily candle feather files and compute quoteVolume. Called once lazily."""
        if self._volume_data is not None:
            return

        data_dir = Path(self._data_source_dir) / "futures"
        if not data_dir.exists():
            # Try without /futures subdirectory
            data_dir = Path(self._data_source_dir)

        # Prefer 1d candles, fall back to 4h
        glob_pattern = f"*{self._pair_suffix}-1d-futures.feather"
        files = list(data_dir.glob(glob_pattern))
        resample_from_subdaily = False

        if not files:
            glob_pattern = f"*{self._pair_suffix}-4h-futures.feather"
            files = list(data_dir.glob(glob_pattern))
            resample_from_subdaily = True
            if files:
                logger.info(
                    "HistoricalVolumePairList: no 1d data found, using 4h (resampled to daily)"
                )

        if not files:
            logger.warning(
                f"HistoricalVolumePairList: no feather files found in {data_dir} "
                f"matching *{self._pair_suffix}-*-futures.feather"
            )
            self._volume_data = pd.DataFrame()
            return

        volume_series: dict[str, pd.Series] = {}

        for f in sorted(files):
            ticker = self._extract_ticker(f.name)
            pair = self._ticker_to_pair(ticker)

            try:
                df = pd.read_feather(f)
            except Exception as e:
                logger.warning(f"HistoricalVolumePairList: error reading {f.name}: {e}")
                continue

            if "date" not in df.columns or "volume" not in df.columns:
                continue

            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

            # Resample sub-daily to daily if needed
            if resample_from_subdaily and len(df) > 0:
                df = df.set_index("date")
                df = (
                    df.resample("1D")
                    .agg(
                        {
                            "open": "first",
                            "high": "max",
                            "low": "min",
                            "close": "last",
                            "volume": "sum",
                        }
                    )
                    .dropna(subset=["close"])
                    .reset_index()
                )

            # quoteVolume = volume * typical_price (matches Freqtrade VolumePairList)
            typical_price = (df["high"] + df["low"] + df["close"]) / 3
            df["quoteVolume"] = df["volume"] * typical_price
            volume_series[pair] = df.set_index("date")["quoteVolume"]

        self._volume_data = pd.DataFrame(volume_series)

        if not self._volume_data.empty:
            logger.info(
                f"HistoricalVolumePairList: loaded {len(volume_series)} volume series "
                f"from {data_dir}, date range "
                f"{self._volume_data.index.min()} to {self._volume_data.index.max()}"
            )
        else:
            logger.warning("HistoricalVolumePairList: no volume data loaded")

    def _build_daily_rankings(self) -> None:
        """Pre-compute daily sorted pair lists. Called once, results cached."""
        if self._daily_rankings is not None:
            return

        self._load_volume_data()

        if self._volume_data is None or self._volume_data.empty:
            self._daily_rankings = {}
            return

        rolling_vol = self._volume_data.rolling(
            window=self._lookback_days, min_periods=1
        ).sum()

        self._daily_rankings = {}

        for date in rolling_vol.index:
            day_str = str(date.date())
            vol_row = rolling_vol.loc[date].dropna()

            # Filter by minimum quoteVolume
            if self._min_value > 0:
                vol_row = vol_row[vol_row >= self._min_value]

            # Sort descending, take top N
            sorted_pairs = list(
                vol_row.sort_values(ascending=False).head(self._number_assets).index
            )
            self._daily_rankings[day_str] = sorted_pairs

        logger.info(
            f"HistoricalVolumePairList: built rankings for {len(self._daily_rankings)} days, "
            f"top_n={self._number_assets}, lookback={self._lookback_days}, "
            f"min_value={self._min_value}"
        )

    # ------------------------------------------------------------------
    # Pairlist interface
    # ------------------------------------------------------------------

    def filter_pairlist(self, pairlist: list[str], tickers: Tickers) -> list[str]:
        """
        Sort and filter pairlist by historical volume.

        Receives the trading universe, looks up volume for each pair,
        and returns the top N sorted by volume (highest first).
        """
        self._build_daily_rankings()

        # Get current backtest time from pairlist manager
        current_time = self._pairlistmanager._current_time
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        day_str = str(current_time.date())

        # Find the closest available date
        ranked_pairs = self._daily_rankings.get(day_str)
        if ranked_pairs is None:
            available_dates = sorted(self._daily_rankings.keys())
            earlier = [d for d in available_dates if d <= day_str]
            if earlier:
                ranked_pairs = self._daily_rankings[earlier[-1]]
            else:
                return pairlist

        # Intersection: keep only pairs in the incoming pairlist, in volume sort order
        pairset = set(pairlist)
        result = [p for p in ranked_pairs if p in pairset]

        logger.info(
            f"HistoricalVolumePairList [{day_str}]: "
            f"{len(pairlist)} in -> {len(result)} out. "
            f"Top 5: {[p.split('/')[0] for p in result[:5]]}"
        )

        return result
