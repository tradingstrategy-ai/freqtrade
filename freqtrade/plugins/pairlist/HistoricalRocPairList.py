"""
Historical ROC (Rate of Change) PairList - Backtestable momentum sorting from local feather files.

Reads historical daily candle feather files from a data directory and sorts pairs
by ROC (percent change over a lookback period). Supports backtesting with time-aware
daily rankings.

Works with any exchange's own data or cross-venue data (e.g. Binance data for GMX).

Usage in config (as a filter after StaticPairList and/or HistoricalVolumePairList):
    "pairlists": [
        {"method": "StaticPairList"},
        {
            "method": "HistoricalVolumePairList",
            "data_source_dir": "user_data/data/binance",
            "number_assets": 20,
            "lookback_days": 25
        },
        {
            "method": "HistoricalRocPairList",
            "data_source_dir": "user_data/data/binance",
            "number_assets": 10,
            "lookback_days": 10,
            "sort_direction": "desc"
        }
    ],
    "enable_dynamic_pairlist": true

pair_suffix, subdirectory, and glob patterns are auto-derived from stake_currency
and trading_mode. Override pair_suffix explicitly only if needed.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from freqtrade.exchange.exchange_types import Tickers
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter, SupportsBacktesting


logger = logging.getLogger(__name__)


class HistoricalRocPairList(IPairList):
    """
    Sort pairs by historical Rate of Change (ROC) from local feather files.

    Receives a pairlist (e.g. from StaticPairList or HistoricalVolumePairList),
    looks up each pair's ROC from the data source, and returns the top N pairs
    sorted by momentum. Time-aware during backtesting via pairlist manager's _current_time.
    """

    supports_backtesting = SupportsBacktesting.YES

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._data_source_dir: str = self._pairlistconfig.get("data_source_dir", "")
        if not self._data_source_dir:
            raise ValueError(
                "HistoricalRocPairList requires 'data_source_dir' in config "
                "(e.g. 'user_data/data/hyperliquid')"
            )

        self._number_assets: int = self._pairlistconfig.get("number_assets", 10)
        self._lookback_days: int = self._pairlistconfig.get("lookback_days", 30)
        self._min_value: float | None = self._pairlistconfig.get("min_value", None)
        self._max_value: float | None = self._pairlistconfig.get("max_value", None)
        self._sort_direction: str = self._pairlistconfig.get("sort_direction", "desc")
        self._token_mapping: dict[str, str] = self._pairlistconfig.get("token_mapping", {})

        if self._sort_direction not in ("asc", "desc"):
            raise ValueError(
                f"HistoricalRocPairList: sort_direction must be 'asc' or 'desc', "
                f"got '{self._sort_direction}'"
            )

        # Derive quote currency and trading mode from config
        self._stake_currency: str = self._config.get("stake_currency", "USDC")
        trading_mode = self._config.get("trading_mode", "spot")

        # Auto-derive pair_suffix from stake_currency + trading_mode when not explicitly set
        if trading_mode == "futures":
            default_suffix = f"_{self._stake_currency}_{self._stake_currency}"
        else:
            default_suffix = f"_{self._stake_currency}"
        self._pair_suffix: str = self._pairlistconfig.get("pair_suffix", default_suffix)

        # Auto-derive candle type string for file paths
        self._candle_type_str: str = "futures" if trading_mode == "futures" else ""

        # Lazy-loaded data
        self._close_data: pd.DataFrame | None = None
        self._daily_rankings: dict[str, list[str]] | None = None

    @property
    def needstickers(self) -> bool:
        return False

    def short_desc(self) -> str:
        return (
            f"{self.name} - Top {self._number_assets} by ROC "
            f"(lookback={self._lookback_days}d, sort={self._sort_direction}, "
            f"source={self._data_source_dir})"
        )

    @staticmethod
    def description() -> str:
        return "Sort pairs by historical Rate of Change (momentum) from local feather files."

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
                "default": 10,
                "description": "Number of assets",
                "help": "Maximum number of pairs to return, sorted by ROC.",
            },
            "lookback_days": {
                "type": "number",
                "default": 30,
                "description": "ROC lookback days",
                "help": "Number of days for ROC calculation (close[D-1] vs close[D-1-lookback]).",
            },
            "min_value": {
                "type": "number",
                "default": None,
                "description": "Minimum ROC",
                "help": "Minimum ROC to include a pair (e.g. 0.0 for only positive momentum).",
            },
            "max_value": {
                "type": "number",
                "default": None,
                "description": "Maximum ROC",
                "help": "Maximum ROC to include a pair (e.g. 1.0 to filter extreme pumps).",
            },
            "sort_direction": {
                "type": "string",
                "default": "desc",
                "description": "Sort direction (desc=trend/breakout, asc=mean-reversion)",
                "help": "'desc' for highest ROC first (trend following), 'asc' for lowest first (mean-reversion).",
            },
            "pair_suffix": {
                "type": "string",
                "default": "",
                "description": "Source file pair suffix (auto-derived if empty)",
                "help": "File naming suffix. Auto-derived from stake_currency + trading_mode if not set.",
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

    def _load_close_data(self) -> None:
        """Load daily candle feather files and extract close prices. Called once lazily."""
        if self._close_data is not None:
            return

        if self._candle_type_str:
            data_dir = Path(self._data_source_dir) / self._candle_type_str
            if not data_dir.exists():
                data_dir = Path(self._data_source_dir)
        else:
            data_dir = Path(self._data_source_dir)

        # Prefer 1d candles, fall back to 4h
        # Skip symlinks to avoid double-counting aliased tickers
        # Skip XYZ-* files (stock/commodity wrappers) to keep rankings crypto-only
        candle_suffix = f"-{self._candle_type_str}" if self._candle_type_str else ""
        glob_pattern = f"*{self._pair_suffix}-1d{candle_suffix}.feather"
        files = [
            f for f in data_dir.glob(glob_pattern)
            if not f.is_symlink() and not f.name.startswith("XYZ-")
        ]
        resample_from_subdaily = False

        if not files:
            glob_pattern = f"*{self._pair_suffix}-4h{candle_suffix}.feather"
            files = [
                f for f in data_dir.glob(glob_pattern)
                if not f.is_symlink() and not f.name.startswith("XYZ-")
            ]
            resample_from_subdaily = True
            if files:
                logger.info(
                    "HistoricalRocPairList: no 1d data found, using 4h (resampled to daily)"
                )

        if not files:
            logger.warning(
                f"HistoricalRocPairList: no feather files found in {data_dir} "
                f"matching *{self._pair_suffix}-*{candle_suffix}.feather"
            )
            self._close_data = pd.DataFrame()
            return

        close_series: dict[str, pd.Series] = {}

        for f in sorted(files):
            ticker = self._extract_ticker(f.name)
            pair = self._ticker_to_pair(ticker)

            try:
                df = pd.read_feather(f)
            except Exception as e:
                logger.warning(f"HistoricalRocPairList: error reading {f.name}: {e}")
                continue

            if "date" not in df.columns or "close" not in df.columns:
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

            close_series[pair] = df.set_index("date")["close"]

        self._close_data = pd.DataFrame(close_series)

        if not self._close_data.empty:
            logger.info(
                f"HistoricalRocPairList: loaded {len(close_series)} close price series "
                f"from {data_dir}, date range "
                f"{self._close_data.index.min()} to {self._close_data.index.max()}"
            )
        else:
            logger.warning("HistoricalRocPairList: no close price data loaded")

    def _build_daily_rankings(self) -> None:
        """Pre-compute daily sorted pair lists by ROC. Called once, results cached."""
        if self._daily_rankings is not None:
            return

        self._load_close_data()

        if self._close_data is None or self._close_data.empty:
            self._daily_rankings = {}
            return

        # ROC = pct_change over lookback_days, shifted by 1 to prevent lookahead bias.
        # For day D, ROC = (close[D-1] - close[D-1-lookback]) / close[D-1-lookback]
        # .shift(1) ensures we never use day D's close when ranking day D.
        roc = self._close_data.pct_change(periods=self._lookback_days, fill_method=None).shift(1)

        ascending = self._sort_direction == "asc"
        self._daily_rankings = {}

        for date in roc.index:
            day_str = str(date.date())
            roc_row = roc.loc[date].dropna()

            # Filter by min/max ROC thresholds
            if self._min_value is not None:
                roc_row = roc_row[roc_row >= self._min_value]
            if self._max_value is not None:
                roc_row = roc_row[roc_row <= self._max_value]

            # Sort all pairs (number_assets applied later at intersection time)
            sorted_pairs = list(
                roc_row.sort_values(ascending=ascending).index
            )
            self._daily_rankings[day_str] = sorted_pairs

        logger.info(
            f"HistoricalRocPairList: built rankings for {len(self._daily_rankings)} days, "
            f"output_limit={self._number_assets}, lookback={self._lookback_days}, "
            f"sort={self._sort_direction}"
        )

    # ------------------------------------------------------------------
    # Pairlist interface
    # ------------------------------------------------------------------

    def filter_pairlist(self, pairlist: list[str], tickers: Tickers) -> list[str]:
        """
        Sort and filter pairlist by historical ROC.

        Receives the trading universe, looks up ROC for each pair,
        and returns the top N sorted by momentum.
        """
        try:
            return self._filter_pairlist_inner(pairlist)
        except Exception as e:
            logger.warning(
                f"HistoricalRocPairList: error filtering, passing through: {e}"
            )
            return pairlist

    def _filter_pairlist_inner(self, pairlist: list[str]) -> list[str]:
        """Inner filtering logic, separated for graceful error handling."""
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

        # Intersection: keep only pairs in the incoming pairlist, in ROC sort order.
        # Apply number_assets limit here (not during ranking) so the filter works
        # correctly when stacked after other plugins that reduce the pairlist.
        # Case-insensitive matching handles k-prefix tokens (kPEPE vs KPEPE) and
        # other casing mismatches between filenames and whitelist.
        pairset_lower = {p.lower(): p for p in pairlist}
        result = [
            pairset_lower[rp.lower()]
            for rp in ranked_pairs
            if rp.lower() in pairset_lower
        ][:self._number_assets]

        logger.info(
            f"HistoricalRocPairList [{day_str}]: "
            f"{len(pairlist)} in -> {len(result)} out. "
            f"Top 5: {[p.split('/')[0] for p in result[:5]]}"
        )

        return result
