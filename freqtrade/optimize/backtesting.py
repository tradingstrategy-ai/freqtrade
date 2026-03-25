# pragma pylint: disable=missing-docstring, W0212, too-many-arguments

"""
This module contains the backtesting logic
"""

import json
import logging
from collections import defaultdict
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from numpy import isnan, nan
from pandas import DataFrame, Series

from freqtrade import constants
from freqtrade.configuration import TimeRange, validate_config_consistency
from freqtrade.constants import DATETIME_PRINT_FORMAT, Config, IntOrInf, LongShort
from freqtrade.data import history
from freqtrade.data.btanalysis import (
    find_existing_backtest_stats,
    get_tick_size_over_time,
    trade_list_to_dataframe,
)
from freqtrade.data.converter import trim_dataframe, trim_dataframes
from freqtrade.data.dataprovider import DataProvider
from freqtrade.data.metrics import combined_dataframes_with_rel_mean
from freqtrade.enums import (
    BacktestState,
    CandleType,
    ExitCheckTuple,
    ExitType,
    MarginMode,
    RunMode,
    TradingMode,
)
from freqtrade.exceptions import DependencyException, OperationalException
from freqtrade.exchange import (
    amount_to_contract_precision,
    price_to_precision,
    timeframe_to_seconds,
)
from freqtrade.exchange.exchange import TICK_SIZE, Exchange
from freqtrade.ft_types import (
    BacktestContentType,
    BacktestContentTypeIcomplete,
    BacktestResultType,
    get_BacktestResultType_default,
)
from freqtrade.leverage.liquidation_price import update_liquidation_prices
from freqtrade.mixins import LoggingMixin
from freqtrade.optimize.backtest_caching import get_strategy_run_id
from freqtrade.optimize.bt_progress import BTProgress
from freqtrade.optimize.optimize_reports import (
    generate_backtest_stats,
    generate_rejected_signals,
    generate_trade_signal_candles,
    show_backtest_results,
    store_backtest_results,
)
from freqtrade.persistence import (
    CustomDataWrapper,
    LocalTrade,
    Order,
    PairLocks,
    Trade,
    disable_database_use,
    enable_database_use,
)
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.resolvers import ExchangeResolver, StrategyResolver
from freqtrade.strategy.interface import IStrategy
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import FtPrecise, dt_now
from freqtrade.util.migrations import migrate_data
from freqtrade.wallets import Wallets


logger = logging.getLogger(__name__)

# Indexes for backtest tuples
DATE_IDX = 0
OPEN_IDX = 1
HIGH_IDX = 2
LOW_IDX = 3
CLOSE_IDX = 4
LONG_IDX = 5
ELONG_IDX = 6  # Exit long
SHORT_IDX = 7
ESHORT_IDX = 8  # Exit short
ENTER_TAG_IDX = 9
EXIT_TAG_IDX = 10
PHASE1_NETTING_INTENTS_IDX = 11
PHASE1_NETTING_EXIT_INTENTS_IDX = 12

# Every change to this headers list must evaluate further usages of the resulting tuple
# and eventually change the constants for indexes at the top
HEADERS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "enter_long",
    "exit_long",
    "enter_short",
    "exit_short",
    "enter_tag",
    "exit_tag",
    "phase1_netting_intents",
    "phase1_netting_exit_intents",
]


class Backtesting:
    """
    Backtesting class, this class contains all the logic to run a backtest

    To run a backtest:
    backtesting = Backtesting(config)
    backtesting.start()
    """

    def __init__(self, config: Config, exchange: Exchange | None = None) -> None:
        LoggingMixin.show_output = False
        self.config = config
        self.results: BacktestResultType = get_BacktestResultType_default()
        self.trade_id_counter: int = 0
        self.order_id_counter: int = 0

        self.config["dry_run"] = True
        self.price_pair_prec: dict[str, Series] = {}
        self.available_pairs: list[str] = []
        self.run_ids: dict[str, str] = {}
        self.strategylist: list[IStrategy] = []
        self.all_bt_content: dict[str, BacktestContentType] = {}
        self.analysis_results: dict[str, dict[str, DataFrame]] = {
            "signals": {},
            "rejected": {},
            "exited": {},
        }
        self.rejected_dict: dict[str, list] = {}

        self._exchange_name = self.config["exchange"]["name"]
        self.__initial_backtest = exchange is None
        if not exchange:
            exchange = ExchangeResolver.load_exchange(self.config, load_leverage_tiers=True)
        self.exchange = exchange

        self.dataprovider = DataProvider(self.config, self.exchange)

        if self.config.get("strategy_list"):
            if self.config.get("freqai", {}).get("enabled", False):
                logger.warning(
                    "Using --strategy-list with FreqAI REQUIRES all strategies "
                    "to have identical feature_engineering_* functions."
                )
            for strat in list(self.config["strategy_list"]):
                stratconf = deepcopy(self.config)
                stratconf["strategy"] = strat
                self.strategylist.append(StrategyResolver.load_strategy(stratconf))
                validate_config_consistency(stratconf)

        else:
            # No strategy list specified, only one strategy
            self.strategylist.append(StrategyResolver.load_strategy(self.config))
            validate_config_consistency(self.config)

        if "timeframe" not in self.config:
            raise OperationalException(
                "Timeframe needs to be set in either "
                "configuration or as cli argument `--timeframe 5m`"
            )
        self.timeframe = str(self.config.get("timeframe"))
        self.timeframe_secs = timeframe_to_seconds(self.timeframe)
        self.timeframe_min = self.timeframe_secs // 60
        self.timeframe_td = timedelta(seconds=self.timeframe_secs)
        self.disable_database_use()
        self.init_backtest_detail()
        self.pairlists = PairListManager(self.exchange, self.config, self.dataprovider)
        self._validate_pairlists_for_backtesting()

        self.dataprovider.add_pairlisthandler(self.pairlists)
        self.dynamic_pairlist: bool = self.config.get("enable_dynamic_pairlist", False)
        self.pairlists.refresh_pairlist(only_first=self.dynamic_pairlist)

        if len(self.pairlists.whitelist) == 0:
            raise OperationalException("No pair in whitelist.")
        self.set_fee()
        self.precision_mode = self.exchange.precisionMode
        self.precision_mode_price = self.exchange.precision_mode_price

        if self.config.get("freqai_backtest_live_models", False):
            from freqtrade.freqai.utils import get_timerange_backtest_live_models

            self.config["timerange"] = get_timerange_backtest_live_models(self.config)

        self.timerange = TimeRange.parse_timerange(
            None if self.config.get("timerange") is None else str(self.config.get("timerange"))
        )

        # Get maximum required startup period
        self.required_startup = max([strat.startup_candle_count for strat in self.strategylist])
        self.exchange.validate_required_startup_candles(self.required_startup, self.timeframe)

        # Add maximum startup candle count to configuration for informative pairs support
        self.config["startup_candle_count"] = self.required_startup

        if self.config.get("freqai", {}).get("enabled", False):
            # For FreqAI, increase the required_startup to includes the training data
            # This value should NOT be written to startup_candle_count
            self.required_startup = self.dataprovider.get_required_startup(self.timeframe)

        self.trading_mode: TradingMode = self.config.get("trading_mode", TradingMode.SPOT)
        self.margin_mode: MarginMode = self.config.get("margin_mode", MarginMode.ISOLATED)
        # strategies which define "can_short=True" will fail to load in Spot mode.
        self._can_short = self.trading_mode != TradingMode.SPOT
        self._position_stacking: bool = self.config.get("position_stacking", False)
        self.enable_protections: bool = self.config.get("enable_protections", False)
        migrate_data(config, self.exchange)

        self.init_backtest()

    def _validate_pairlists_for_backtesting(self):
        if "VolumePairList" in self.pairlists.name_list:
            raise OperationalException(
                "VolumePairList not allowed for backtesting. Please use StaticPairList instead."
            )

        if len(self.strategylist) > 1 and "PrecisionFilter" in self.pairlists.name_list:
            raise OperationalException(
                "PrecisionFilter not allowed for backtesting multiple strategies."
            )

    def log_once(self, msg: str) -> None:
        """
        Partial reimplementation of log_once from the Login mixin.
        only used by recursive, as __initial_backtest is false in all other cases.

        """
        if self.__initial_backtest:
            logger.info(msg)

    def set_fee(self):
        if self.config.get("fee", None) is not None:
            self.fee = self.config["fee"]
            self.log_once(f"Using fee {self.fee:.4%} from config.")
        else:
            fees = [
                self.exchange.get_fee(
                    symbol=self.pairlists.whitelist[0],
                    taker_or_maker=mt,
                )
                for mt in ("taker", "maker")
            ]
            self.fee = max(fee for fee in fees if fee is not None)
            self.log_once(f"Using fee {self.fee:.4%} - worst case fee from exchange (lowest tier).")

    @staticmethod
    def cleanup():
        LoggingMixin.show_output = True
        enable_database_use()

    def init_backtest_detail(self) -> None:
        # Load detail timeframe if specified
        self.timeframe_detail = str(self.config.get("timeframe_detail", ""))
        if self.timeframe_detail:
            timeframe_detail_secs = timeframe_to_seconds(self.timeframe_detail)
            self.timeframe_detail_td = timedelta(seconds=timeframe_detail_secs)
            if self.timeframe_secs <= timeframe_detail_secs:
                raise OperationalException(
                    "Detail timeframe must be smaller than strategy timeframe."
                )

        else:
            self.timeframe_detail_td = timedelta(seconds=0)
        self.detail_data: dict[str, DataFrame] = {}
        self.futures_data: dict[str, DataFrame] = {}

    def init_backtest(self):
        self.prepare_backtest(False)

        self.wallets = Wallets(self.config, self.exchange, is_backtest=True)

        self.progress = BTProgress()
        self.abort = False

    def _set_strategy(self, strategy: IStrategy):
        """
        Load strategy into backtesting
        """
        self.strategy: IStrategy = strategy
        strategy.dp = self.dataprovider
        # Attach Wallets to Strategy baseclass
        strategy.wallets = self.wallets
        # Set stoploss_on_exchange to false for backtesting,
        # since a "perfect" stoploss-exit is assumed anyway
        # And the regular "stoploss" function would not apply to that case
        self.strategy.order_types["stoploss_on_exchange"] = False
        # Update can_short flag
        self._can_short = self.trading_mode != TradingMode.SPOT and strategy.can_short

        self.strategy.ft_bot_start()

    def _load_protections(self, strategy: IStrategy):
        if self.config.get("enable_protections", False):
            self.protections = ProtectionManager(self.config, strategy.protections)

    def load_bt_data(self) -> tuple[dict[str, DataFrame], TimeRange]:
        """
        Loads backtest data and returns the data combined with the timerange
        as tuple.
        """
        self.progress.init_step(BacktestState.DATALOAD, 1)

        data = history.load_data(
            datadir=self.config["datadir"],
            pairs=self.pairlists.whitelist,
            timeframe=self.timeframe,
            timerange=self.timerange,
            startup_candles=self.required_startup,
            fail_without_data=True,
            data_format=self.config["dataformat_ohlcv"],
            candle_type=self.config.get("candle_type_def", CandleType.SPOT),
        )

        min_date, max_date = history.get_timerange(data)

        logger.info(
            f"Loading data from {min_date.strftime(DATETIME_PRINT_FORMAT)} "
            f"up to {max_date.strftime(DATETIME_PRINT_FORMAT)} "
            f"({(max_date - min_date).days} days)."
        )

        # Adjust startts forward if not enough data is available
        self.timerange.adjust_start_if_necessary(
            timeframe_to_seconds(self.timeframe), self.required_startup, min_date
        )

        self.progress.set_new_value(1)
        self._load_bt_data_detail()
        self.price_pair_prec = {}
        self.available_pairs = []
        for pair in self.pairlists.whitelist:
            if pair in data:
                # Load price precision logic
                self.price_pair_prec[pair] = get_tick_size_over_time(data[pair])
                self.available_pairs.append(pair)
        return data, self.timerange

    def _load_bt_data_detail(self) -> None:
        """
        Loads backtest detail data (smaller timeframe) if necessary.
        """
        if self.timeframe_detail:
            self.detail_data = history.load_data(
                datadir=self.config["datadir"],
                pairs=self.pairlists.whitelist,
                timeframe=self.timeframe_detail,
                timerange=self.timerange,
                startup_candles=0,
                fail_without_data=True,
                data_format=self.config["dataformat_ohlcv"],
                candle_type=self.config.get("candle_type_def", CandleType.SPOT),
            )
        else:
            self.detail_data = {}
        if self.trading_mode == TradingMode.FUTURES:
            funding_fee_timeframe: str = self.exchange.get_option("funding_fee_timeframe")
            self.funding_fee_timeframe_secs: int = timeframe_to_seconds(funding_fee_timeframe)
            mark_timeframe: str = self.exchange.get_option("mark_ohlcv_timeframe")

            # Load additional futures data.
            funding_rates_dict = history.load_data(
                datadir=self.config["datadir"],
                pairs=self.pairlists.whitelist,
                timeframe=funding_fee_timeframe,
                timerange=self.timerange,
                startup_candles=0,
                fail_without_data=True,
                data_format=self.config["dataformat_ohlcv"],
                candle_type=CandleType.FUNDING_RATE,
            )

            # For simplicity, assign to CandleType.Mark (might contain index candles!)
            mark_rates_dict = history.load_data(
                datadir=self.config["datadir"],
                pairs=self.pairlists.whitelist,
                timeframe=mark_timeframe,
                timerange=self.timerange,
                startup_candles=0,
                fail_without_data=True,
                data_format=self.config["dataformat_ohlcv"],
                candle_type=CandleType.from_string(self.exchange.get_option("mark_ohlcv_price")),
            )
            # Combine data to avoid combining the data per trade.
            unavailable_pairs = []
            uses_leverage_tiers = self.exchange.get_option("uses_leverage_tiers", True)
            for pair in self.pairlists.whitelist:
                if uses_leverage_tiers and pair not in self.exchange._leverage_tiers:
                    unavailable_pairs.append(pair)
                    continue

                self.futures_data[pair] = self.exchange.combine_funding_and_mark(
                    funding_rates=funding_rates_dict[pair],
                    mark_rates=mark_rates_dict[pair],
                    futures_funding_rate=self.config.get("futures_funding_rate", None),
                )

            if unavailable_pairs:
                raise OperationalException(
                    f"Pairs {', '.join(unavailable_pairs)} got no leverage tiers available. "
                    "It is therefore impossible to backtest with this pair at the moment."
                )
        else:
            self.futures_data = {}

    def get_pair_precision(self, pair: str, current_time: datetime) -> tuple[float | None, int]:
        """
        Get pair precision at that moment in time
        :param pair: Pair to get precision for
        :param current_time: Time to get precision for
        :return: tuple of price precision, precision_mode_price for the pair at that given time.
        """
        precision_series = self.price_pair_prec.get(pair)
        if precision_series is not None:
            precision = precision_series.asof(current_time)

            if not isnan(precision):
                # Force tick size if we define the precision
                return precision, TICK_SIZE
        return self.exchange.get_precision_price(pair), self.precision_mode_price

    def disable_database_use(self):
        disable_database_use(self.timeframe)

    def prepare_backtest(self, enable_protections):
        """
        Backtesting setup method - called once for every call to "backtest()".
        """
        self.disable_database_use()
        PairLocks.reset_locks()
        Trade.reset_trades()
        CustomDataWrapper.reset_custom_data()
        self.rejected_trades = 0
        self.timedout_entry_orders = 0
        self.timedout_exit_orders = 0
        self.canceled_trade_entries = 0
        self.canceled_entry_orders = 0
        self.replaced_entry_orders = 0
        self.canceled_exit_orders = 0
        self.replaced_exit_orders = 0
        self.dataprovider.clear_cache()
        if enable_protections:
            self._load_protections(self.strategy)

    def check_abort(self):
        """
        Check if abort was requested, raise DependencyException if that's the case
        Only applies to Interactive backtest mode (webserver mode)
        """
        if self.abort:
            self.abort = False
            raise DependencyException("Stop requested")

    def _get_ohlcv_as_lists(self, processed: dict[str, DataFrame]) -> dict[str, tuple]:
        """
        Helper function to convert a processed dataframes into lists for performance reasons.

        Used by backtest() - so keep this optimized for performance.

        :param processed: a processed dictionary with format {pair, data}, which gets cleared to
        optimize memory usage!
        """

        data: dict = {}
        self.progress.init_step(BacktestState.CONVERT, len(processed))

        # Create dict with data
        for pair in processed.keys():
            pair_data = processed[pair]
            self.check_abort()
            self.progress.increment()

            if not pair_data.empty:
                # Cleanup from prior runs
                pair_data.drop(HEADERS[5:] + ["buy", "sell"], axis=1, errors="ignore")
            df_analyzed = self.strategy.ft_advise_signals(pair_data, {"pair": pair})
            # Update dataprovider cache
            self.dataprovider._set_cached_df(
                pair, self.timeframe, df_analyzed, self.config["candle_type_def"]
            )

            # Trim startup period from analyzed dataframe
            df_analyzed = processed[pair] = pair_data = trim_dataframe(
                df_analyzed, self.timerange, startup_candles=self.required_startup
            )

            # Create a copy of the dataframe before shifting, that way the entry signal/tag
            # remains on the correct candle for callbacks.
            df_analyzed = df_analyzed.copy()

            # To avoid using data from future, we use entry/exit signals shifted
            # from the previous candle
            for col in HEADERS[5:]:
                tag_col = col in (
                    "enter_tag",
                    "exit_tag",
                    "phase1_netting_intents",
                    "phase1_netting_exit_intents",
                )
                if col in df_analyzed.columns:
                    df_analyzed[col] = (
                        df_analyzed.loc[:, col]
                        .replace([nan], [0 if not tag_col else None])
                        .shift(1)
                    )
                elif not df_analyzed.empty:
                    df_analyzed[col] = 0 if not tag_col else None

            df_analyzed = df_analyzed.drop(df_analyzed.head(1).index)

            # Convert from Pandas to list for performance reasons
            # (Looping Pandas is slow.)
            data[pair] = df_analyzed[HEADERS].values.tolist() if not df_analyzed.empty else []
        return data

    def _get_close_rate(
        self,
        row: tuple,
        trade: LocalTrade,
        current_time: datetime,
        exit_: ExitCheckTuple,
        trade_dur: int,
    ) -> float:
        """
        Get close rate for backtesting result
        """
        # Special handling if high or low hit STOP_LOSS or ROI
        if exit_.exit_type in (
            ExitType.STOP_LOSS,
            ExitType.TRAILING_STOP_LOSS,
            ExitType.LIQUIDATION,
        ):
            return self._get_close_rate_for_stoploss(row, trade, exit_, trade_dur)
        elif exit_.exit_type == (ExitType.ROI):
            return self._get_close_rate_for_roi(row, trade, current_time, exit_, trade_dur)
        else:
            return row[OPEN_IDX]

    def _get_close_rate_for_stoploss(
        self, row: tuple, trade: LocalTrade, exit_: ExitCheckTuple, trade_dur: int
    ) -> float:
        # our stoploss was already lower than candle high,
        # possibly due to a cancelled trade exit.
        # exit at open price.
        is_short = trade.is_short or False
        leverage = trade.leverage or 1.0
        side_1 = -1 if is_short else 1
        if exit_.exit_type == ExitType.LIQUIDATION and trade.liquidation_price:
            stoploss_value = trade.liquidation_price
        else:
            stoploss_value = trade.stop_loss

        if is_short:
            if stoploss_value < row[LOW_IDX]:
                return row[OPEN_IDX]
        else:
            if stoploss_value > row[HIGH_IDX]:
                return row[OPEN_IDX]

        # Special case: trailing triggers within same candle as trade opened. Assume most
        # pessimistic price movement, which is moving just enough to arm stoploss and
        # immediately going down to stop price.
        if exit_.exit_type == ExitType.TRAILING_STOP_LOSS and trade_dur == 0:
            if (
                not self.strategy.use_custom_stoploss
                and self.strategy.trailing_stop
                and self.strategy.trailing_only_offset_is_reached
                and self.strategy.trailing_stop_positive_offset is not None
                and self.strategy.trailing_stop_positive
            ):
                # Worst case: price reaches stop_positive_offset and dives down.
                stop_rate = row[OPEN_IDX] * (
                    1
                    + side_1 * abs(self.strategy.trailing_stop_positive_offset)
                    - side_1 * abs(self.strategy.trailing_stop_positive / leverage)
                )
            else:
                # Worst case: price ticks tiny bit above open and dives down.
                stop_rate = row[OPEN_IDX] * (
                    1 - side_1 * abs((trade.stop_loss_pct or 0.0) / leverage)
                )

            # Limit lower-end to candle low to avoid exits below the low.
            # This still remains "worst case" - but "worst realistic case".
            if is_short:
                return min(row[HIGH_IDX], stop_rate)
            else:
                return max(row[LOW_IDX], stop_rate)

        # Set close_rate to stoploss
        return stoploss_value

    def _get_close_rate_for_roi(
        self,
        row: tuple,
        trade: LocalTrade,
        current_time: datetime,
        exit_: ExitCheckTuple,
        trade_dur: int,
    ) -> float:
        is_short = trade.is_short or False
        leverage = trade.leverage or 1.0
        side_1 = -1 if is_short else 1
        roi_entry, roi = self.strategy.min_roi_reached_entry(
            trade,  # type: ignore[arg-type]
            trade_dur,
            current_time,
        )
        if roi is not None and roi_entry is not None:
            if roi == -1 and roi_entry % self.timeframe_min == 0:
                # When force_exiting with ROI=-1, the roi time will always be equal to trade_dur.
                # If that entry is a multiple of the timeframe (so on candle open)
                # - we'll use open instead of close
                return row[OPEN_IDX]

            # - (Expected abs profit - open_rate - open_fee) / (fee_close -1)
            roi_rate = trade.open_rate * roi / leverage
            open_fee_rate = side_1 * trade.open_rate * (1 + side_1 * trade.fee_open)
            close_rate = -(roi_rate + open_fee_rate) / ((trade.fee_close or 0.0) - side_1 * 1)
            if is_short:
                is_new_roi = row[OPEN_IDX] < close_rate
            else:
                is_new_roi = row[OPEN_IDX] > close_rate
            if (
                trade_dur > 0
                and trade_dur == roi_entry
                and roi_entry % self.timeframe_min == 0
                and is_new_roi
            ):
                # new ROI entry came into effect.
                # use Open rate if open_rate > calculated exit rate
                return row[OPEN_IDX]

            if trade_dur == 0 and (
                (
                    is_short
                    # Red candle (for longs)
                    and row[OPEN_IDX] < row[CLOSE_IDX]  # Red candle
                    and trade.open_rate > row[OPEN_IDX]  # trade-open above open_rate
                    and close_rate < row[CLOSE_IDX]  # closes below close
                )
                or (
                    not is_short
                    # green candle (for shorts)
                    and row[OPEN_IDX] > row[CLOSE_IDX]  # green candle
                    and trade.open_rate < row[OPEN_IDX]  # trade-open below open_rate
                    and close_rate > row[CLOSE_IDX]  # closes above close
                )
            ):
                # ROI on opening candles with custom pricing can only
                # trigger if the entry was at Open or lower wick.
                # details: https: // github.com/freqtrade/freqtrade/issues/6261
                # If open_rate is < open, only allow exits below the close on red candles.
                raise ValueError("Opening candle ROI on red candles.")

            # Use the maximum between close_rate and low as we
            # cannot exit outside of a candle.
            # Applies when a new ROI setting comes in place and the whole candle is above that.
            return min(max(close_rate, row[LOW_IDX]), row[HIGH_IDX])

        else:
            # This should not be reached...
            return row[OPEN_IDX]

    def _check_adjust_trade_for_candle(
        self, trade: LocalTrade, row: tuple, current_time: datetime
    ) -> LocalTrade:
        current_rate: float = row[OPEN_IDX]
        current_profit = trade.calc_profit_ratio(current_rate)
        min_stake = self.exchange.get_min_pair_stake_amount(trade.pair, current_rate, -0.1)
        max_stake = self.exchange.get_max_pair_stake_amount(trade.pair, current_rate)
        stake_available = self.wallets.get_available_stake_amount()
        stake_amount, order_tag = self.strategy._adjust_trade_position_internal(
            trade=trade,  # type: ignore[arg-type]
            current_time=current_time,
            current_rate=current_rate,
            current_profit=current_profit,
            min_stake=min_stake,
            max_stake=min(max_stake, stake_available),
            current_entry_rate=current_rate,
            current_exit_rate=current_rate,
            current_entry_profit=current_profit,
            current_exit_profit=current_profit,
        )

        # Check if we should increase our position
        if stake_amount is not None and stake_amount > 0.0:
            check_adjust_entry = True
            if self.strategy.max_entry_position_adjustment > -1:
                entry_count = trade.nr_of_successful_entries
                check_adjust_entry = entry_count <= self.strategy.max_entry_position_adjustment
            if check_adjust_entry:
                pos_trade = self._enter_trade(
                    trade.pair,
                    row,
                    "short" if trade.is_short else "long",
                    stake_amount,
                    trade,
                    entry_tag1=order_tag,
                )
                if pos_trade is not None:
                    self.wallets.update()
                    return pos_trade

        if stake_amount is not None and stake_amount < 0.0:
            amount = amount_to_contract_precision(
                abs(
                    float(
                        FtPrecise(stake_amount)
                        * FtPrecise(trade.amount)
                        / FtPrecise(trade.stake_amount)
                    )
                ),
                trade.amount_precision,
                self.precision_mode,
                trade.contract_size,
            )
            if amount == 0.0:
                return trade
            remaining = (trade.amount - amount) * current_rate
            if min_stake and remaining != 0 and remaining < min_stake:
                # Remaining stake is too low to be sold.
                return trade
            exit_ = ExitCheckTuple(ExitType.PARTIAL_EXIT, order_tag)
            pos_trade = self._get_exit_for_signal(trade, row, exit_, current_time, amount)
            if pos_trade is not None:
                order = pos_trade.orders[-1]
                # If the order was filled and for the full trade amount, we need to close the trade.
                self._process_exit_order(order, pos_trade, current_time, row, trade.pair)
                return pos_trade

        return trade

    def _get_order_filled(self, rate: float, row: tuple) -> bool:
        """Rate is within candle, therefore filled"""
        return row[LOW_IDX] <= rate <= row[HIGH_IDX]

    def _call_adjust_stop(self, current_date: datetime, trade: LocalTrade, current_rate: float):
        profit = trade.calc_profit_ratio(current_rate)
        self.strategy.ft_stoploss_adjust(
            current_rate,
            trade,  # type: ignore
            current_date,
            profit,
            0,
            after_fill=True,
        )

    def _try_close_open_order(
        self, order: Order | None, trade: LocalTrade, current_date: datetime, row: tuple
    ) -> bool:
        """
        Check if an order is open and if it should've filled.
        :return:  True if the order filled.
        """
        if order and self._get_order_filled(order.ft_price, row):
            order.close_bt_order(current_date, trade)
            self._run_funding_fees(trade, current_date, force=True)
            strategy_safe_wrapper(self.strategy.order_filled, supress_error=True)(
                pair=trade.pair,
                trade=trade,  # type: ignore[arg-type]
                order=order,
                current_time=current_date,
            )

            if self.margin_mode == MarginMode.CROSS or not (
                order.ft_order_side == trade.exit_side and order.safe_amount == trade.amount
            ):
                # trade is still open or we are in cross margin mode and
                # must update all liquidation prices
                update_liquidation_prices(
                    trade,
                    exchange=self.exchange,
                    wallets=self.wallets,
                    stake_currency=self.config["stake_currency"],
                    dry_run=True,
                )
            if not (order.ft_order_side == trade.exit_side and order.safe_amount == trade.amount):
                self._call_adjust_stop(current_date, trade, order.ft_price)
            return True
        return False

    def _process_exit_order(
        self, order: Order, trade: LocalTrade, current_time: datetime, row: tuple, pair: str
    ):
        """
        Takes an exit order and processes it, potentially closing the trade.
        """
        if self._try_close_open_order(order, trade, current_time, row):
            if (
                not trade.get_custom_data("phase1_pending_exit_plan")
                and Backtesting._phase1_is_full_trade_exit(order, trade)
            ):
                exit_plan = Backtesting._build_phase1_full_trade_exit_plan(trade)
                if exit_plan:
                    trade.set_custom_data("phase1_pending_exit_plan", exit_plan)
            self._apply_phase1_exit_fill(trade, order.ft_price, current_time)
            if self._phase1_trade_fully_closed(trade):
                trade.close_date = current_time
                trade.close(order.ft_price, show_msg=False)
                LocalTrade.close_bt_trade(trade)
                self.wallets.update()
                self.run_protections(pair, current_time, trade.trade_direction)
                return
            sub_trade = order.safe_amount_after_fee != trade.amount
            if sub_trade:
                trade.recalc_trade_from_orders()
            else:
                trade.close_date = current_time
                trade.close(order.ft_price, show_msg=False)

                LocalTrade.close_bt_trade(trade)
            self.wallets.update()
            self.run_protections(pair, current_time, trade.trade_direction)

    @staticmethod
    def _apply_phase1_exit_fill(
        trade: LocalTrade,
        exit_price: float,
        current_time: datetime,
    ) -> None:
        pending_exit_plan = trade.get_custom_data("phase1_pending_exit_plan")
        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not pending_exit_plan or not sleeves:
            return

        closing_ids = set(pending_exit_plan.get("sleeve_ids", []))
        sleeve_exit_map = {
            item["sleeve_id"]: item for item in pending_exit_plan.get("sleeve_exits", [])
        }
        updated_sleeves = []
        closed_sleeves = trade.get_custom_data("phase1_closed_sleeves") or []
        for sleeve in sleeves:
            sleeve_copy = dict(sleeve)
            if sleeve_copy["sleeve_id"] in closing_ids and sleeve_copy["closed_at"] is None:
                planned_exit = sleeve_exit_map.get(sleeve_copy["sleeve_id"])
                current_qty = float(sleeve_copy["quantity"])
                qty = (
                    float(planned_exit["quantity"])
                    if planned_exit is not None
                    else current_qty
                )
                qty = min(qty, current_qty)
                if qty <= 0:
                    updated_sleeves.append(sleeve_copy)
                    continue
                avg_price = float(sleeve_copy["avg_price"])
                if sleeve_copy["side"] == "long":
                    realized = (exit_price - avg_price) * qty
                else:
                    realized = (avg_price - exit_price) * qty
                sleeve_copy["realized_pnl"] = float(sleeve_copy.get("realized_pnl", 0.0)) + realized
                remaining_qty = current_qty - qty
                sleeve_copy["quantity"] = remaining_qty
                sleeve_copy["updated_at"] = current_time.isoformat()
                if "quantity_units" in sleeve_copy:
                    planned_units = (
                        float(planned_exit["quantity_units"])
                        if planned_exit is not None
                        and planned_exit.get("quantity_units") is not None
                        else float(sleeve_copy["quantity_units"])
                    )
                    remaining_units = max(0.0, float(sleeve_copy["quantity_units"]) - planned_units)
                    sleeve_copy["quantity_units"] = remaining_units
                if remaining_qty <= 0.0:
                    sleeve_copy["quantity"] = 0.0
                    sleeve_copy["closed_at"] = current_time.isoformat()
                    closed_sleeves.append(dict(sleeve_copy))
            updated_sleeves.append(sleeve_copy)

        trade.set_custom_data("phase1_sleeves", updated_sleeves)
        trade.set_custom_data("phase1_closed_sleeves", closed_sleeves)
        Backtesting._rebase_phase1_trade_to_single_open_sleeve(trade, updated_sleeves)
        trade.set_custom_data("phase1_pending_exit_plan", None)

    @staticmethod
    def _phase1_trade_fully_closed(trade: LocalTrade) -> bool:
        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not sleeves:
            return False
        return all(
            sleeve.get("closed_at") is not None or float(sleeve.get("quantity", 0.0)) <= 0.0
            for sleeve in sleeves
        )

    @staticmethod
    def _rebase_phase1_trade_to_single_open_sleeve(
        trade: LocalTrade,
        sleeves: list[dict],
    ) -> None:
        open_sleeves = [
            sleeve
            for sleeve in sleeves
            if sleeve.get("closed_at") is None and float(sleeve.get("quantity", 0.0)) > 0.0
        ]
        if len(open_sleeves) != 1:
            return
        remaining_sleeve = open_sleeves[0]
        trade.enter_tag = f"{remaining_sleeve['strategy_name']}|"
        trade.open_rate = float(remaining_sleeve.get("avg_price") or trade.open_rate)
        opened_at = datetime.fromisoformat(remaining_sleeve["opened_at"])
        if opened_at.tzinfo is not None:
            opened_at = opened_at.astimezone(UTC).replace(tzinfo=None)
        trade.open_date = opened_at

    @staticmethod
    def _apply_phase1_entry_adjustment_metadata(
        trade: LocalTrade,
        strategy_name: str,
        side: str,
        added_quantity: float,
        fill_price: float,
        current_time: datetime,
    ) -> None:
        """Attribute a filled position adjustment back to a contributor sleeve."""
        if added_quantity <= 0.0:
            return

        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not sleeves:
            return

        updated_sleeves: list[dict] = []
        matched = False
        for sleeve in sleeves:
            sleeve_copy = dict(sleeve)
            if (
                not matched
                and sleeve_copy.get("strategy_name") == strategy_name
                and sleeve_copy.get("side") == side
                and sleeve_copy.get("closed_at") is None
                and float(sleeve_copy.get("quantity", 0.0)) > 0.0
            ):
                old_qty = float(sleeve_copy.get("quantity", 0.0))
                old_units = float(sleeve_copy.get("quantity_units", old_qty))
                total_qty = old_qty + added_quantity
                old_avg = float(sleeve_copy.get("avg_price", fill_price))
                added_units = (
                    (added_quantity * old_units / old_qty)
                    if old_qty > 0.0 and old_units > 0.0
                    else added_quantity
                )
                sleeve_copy["quantity"] = total_qty
                sleeve_copy["quantity_units"] = old_units + added_units
                sleeve_copy["avg_price"] = (
                    ((old_qty * old_avg) + (added_quantity * fill_price)) / total_qty
                )
                sleeve_copy["updated_at"] = current_time.isoformat()
                matched = True
            updated_sleeves.append(sleeve_copy)

        if not matched:
            updated_sleeves.append(
                {
                    "sleeve_id": f"{trade.pair}|{strategy_name}|{side}|{current_time.isoformat()}",
                    "strategy_name": strategy_name,
                    "pair": trade.pair,
                    "side": side,
                    "quantity": added_quantity,
                    "quantity_units": added_quantity,
                    "avg_price": fill_price,
                    "opened_at": current_time.isoformat(),
                    "updated_at": current_time.isoformat(),
                    "realized_pnl": 0.0,
                    "closed_at": None,
                }
            )

        trade.set_custom_data("phase1_sleeves", updated_sleeves)

        phase1_plan = trade.get_custom_data("phase1_net_plan")
        if isinstance(phase1_plan, dict):
            updated_plan = dict(phase1_plan)
            updated_plan["sleeves"] = updated_sleeves
            updated_plan["strategy_names"] = sorted(
                {
                    str(sleeve["strategy_name"])
                    for sleeve in updated_sleeves
                    if sleeve.get("closed_at") is None and float(sleeve.get("quantity", 0.0)) > 0.0
                }
            )
            updated_plan["contributor_count"] = len(updated_plan["strategy_names"])
            trade.set_custom_data("phase1_net_plan", updated_plan)

    @staticmethod
    def _phase1_is_full_trade_exit(order: Order, trade: LocalTrade) -> bool:
        return abs(float(order.safe_amount_after_fee) - float(trade.amount)) <= 1e-12

    @staticmethod
    def _build_phase1_full_trade_exit_plan(trade: LocalTrade) -> dict | None:
        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not sleeves:
            return None
        side = "short" if trade.is_short else "long"
        open_sleeves = [
            sleeve
            for sleeve in sleeves
            if sleeve.get("side") == side
            and sleeve.get("closed_at") is None
            and float(sleeve.get("quantity", 0.0)) > 0.0
        ]
        if not open_sleeves:
            return None
        strategy_names = sorted(
            {str(sleeve["strategy_name"]) for sleeve in open_sleeves if sleeve.get("strategy_name")}
        )
        sleeve_exits = [
            {
                "sleeve_id": sleeve["sleeve_id"],
                "quantity": float(sleeve["quantity"]),
                "quantity_units": float(sleeve.get("quantity_units", sleeve["quantity"])),
                "close": True,
            }
            for sleeve in open_sleeves
        ]
        exit_amount = sum(float(item["quantity"]) for item in sleeve_exits)
        if exit_amount <= 0.0:
            return None
        return {
            "strategy_names": strategy_names,
            "sleeve_ids": [item["sleeve_id"] for item in sleeve_exits],
            "sleeve_exits": sleeve_exits,
            "exit_amount": exit_amount,
            "exit_reason": f"phase1_sleeve_exit:{','.join(strategy_names)}",
        }

    def _get_exit_for_signal(
        self,
        trade: LocalTrade,
        row: tuple,
        exit_: ExitCheckTuple,
        current_time: datetime,
        amount: float | None = None,
    ) -> LocalTrade | None:
        if exit_.exit_flag:
            trade.close_date = current_time
            exit_reason = exit_.exit_reason
            amount_ = amount if amount is not None else trade.amount
            trade_dur = int((trade.close_date_utc - trade.open_date_utc).total_seconds() // 60)
            try:
                close_rate = self._get_close_rate(row, trade, current_time, exit_, trade_dur)
            except ValueError:
                return None
            # call the custom exit price,with default value as previous close_rate
            current_profit = trade.calc_profit_ratio(close_rate)
            order_type = self.strategy.order_types["exit"]
            if exit_.exit_type in (
                ExitType.EXIT_SIGNAL,
                ExitType.CUSTOM_EXIT,
                ExitType.PARTIAL_EXIT,
            ):
                # Checks and adds an exit tag, after checking that the length of the
                # row has the length for an exit tag column
                if (
                    len(row) > EXIT_TAG_IDX
                    and row[EXIT_TAG_IDX] is not None
                    and len(row[EXIT_TAG_IDX]) > 0
                    and exit_.exit_type in (ExitType.EXIT_SIGNAL,)
                ):
                    exit_reason = row[EXIT_TAG_IDX]
                # Custom exit pricing only for exit-signals
                if order_type == "limit":
                    rate = strategy_safe_wrapper(
                        self.strategy.custom_exit_price, default_retval=close_rate
                    )(
                        pair=trade.pair,
                        trade=trade,  # type: ignore[arg-type]
                        current_time=current_time,
                        proposed_rate=close_rate,
                        current_profit=current_profit,
                        exit_tag=exit_reason,
                    )
                    if rate is not None and rate != close_rate:
                        close_rate = price_to_precision(
                            rate, trade.price_precision, trade.precision_mode_price
                        )
                    # We can't place orders lower than current low.
                    # freqtrade does not support this in live, and the order would fill immediately
                    if trade.is_short:
                        close_rate = min(close_rate, row[HIGH_IDX])
                    else:
                        close_rate = max(close_rate, row[LOW_IDX])
            # Confirm trade exit:
            time_in_force = self.strategy.order_time_in_force["exit"]

            if exit_.exit_type not in (
                ExitType.LIQUIDATION,
                ExitType.PARTIAL_EXIT,
            ) and not strategy_safe_wrapper(self.strategy.confirm_trade_exit, default_retval=True)(
                pair=trade.pair,
                trade=trade,  # type: ignore[arg-type]
                order_type=order_type,
                amount=amount_,
                rate=close_rate,
                time_in_force=time_in_force,
                sell_reason=exit_reason,  # deprecated
                exit_reason=exit_reason,
                current_time=current_time,
            ):
                return None

            trade.exit_reason = exit_reason

            return self._exit_trade(trade, row, close_rate, amount_, exit_reason)
        return None

    def _exit_trade(
        self,
        trade: LocalTrade,
        sell_row: tuple,
        close_rate: float,
        amount: float,
        exit_reason: str | None,
    ) -> LocalTrade | None:
        self.order_id_counter += 1
        exit_candle_time = sell_row[DATE_IDX].to_pydatetime()
        order_type = self.strategy.order_types["exit"]
        # amount = amount or trade.amount
        amount = amount_to_contract_precision(
            amount or trade.amount, trade.amount_precision, self.precision_mode, trade.contract_size
        )

        if self.handle_similar_order(trade, close_rate, amount, trade.exit_side, exit_candle_time):
            return None

        order = Order(
            id=self.order_id_counter,
            ft_trade_id=trade.id,
            order_date=exit_candle_time,
            order_update_date=exit_candle_time,
            ft_is_open=True,
            ft_pair=trade.pair,
            order_id=str(self.order_id_counter),
            symbol=trade.pair,
            ft_order_side=trade.exit_side,
            side=trade.exit_side,
            order_type=order_type,
            status="open",
            ft_price=close_rate,
            price=close_rate,
            average=close_rate,
            amount=amount,
            filled=0,
            remaining=amount,
            cost=amount * close_rate * (1 + self.fee),
            ft_order_tag=exit_reason,
        )
        order._trade_bt = trade
        trade.orders.append(order)
        return trade

    def _check_trade_exit(
        self, trade: LocalTrade, row: tuple, current_time: datetime
    ) -> LocalTrade | None:
        self._run_funding_fees(trade, current_time)

        # Check if we need to adjust our current positions
        if self.strategy.position_adjustment_enable:
            trade = self._check_adjust_trade_for_candle(trade, row, current_time)

        if trade.is_open:
            phase1_exit_trade = self._check_phase1_sleeve_exit(
                trade, row, current_time
            )
            if phase1_exit_trade:
                return phase1_exit_trade
            enter = row[SHORT_IDX] if trade.is_short else row[LONG_IDX]
            exit_sig = row[ESHORT_IDX] if trade.is_short else row[ELONG_IDX]
            exits = self.strategy.should_exit(
                trade,  # type: ignore
                row[OPEN_IDX],
                row[DATE_IDX].to_pydatetime(),
                enter=enter,
                exit_=exit_sig,
                low=row[LOW_IDX],
                high=row[HIGH_IDX],
            )
            for exit_ in exits:
                phase1_owner_exit_trade = self._check_phase1_owner_exit(
                    trade,
                    row,
                    current_time,
                    exit_,
                )
                if phase1_owner_exit_trade:
                    return phase1_owner_exit_trade
                t = self._get_exit_for_signal(trade, row, exit_, current_time)
                if t:
                    return t
        return None

    def _check_phase1_sleeve_exit(
        self, trade: LocalTrade, row: tuple, current_time: datetime
    ) -> LocalTrade | None:
        row_exit_payload = (
            row[PHASE1_NETTING_EXIT_INTENTS_IDX]
            if len(row) >= PHASE1_NETTING_EXIT_INTENTS_IDX + 1
            else None
        )
        exit_payload = self._collect_phase1_live_exit_intents(
            trade,
            row,
            current_time,
            row_exit_payload,
        )
        exit_plan = self._build_phase1_net_exit_plan(trade, exit_payload)
        if not exit_plan:
            return None
        trade.set_custom_data("phase1_pending_exit_plan", exit_plan)
        trade.exit_reason = exit_plan["exit_reason"]
        return self._exit_trade(
            trade,
            row,
            row[OPEN_IDX],
            exit_plan["exit_amount"],
            exit_plan["exit_reason"],
        )

    def _collect_phase1_live_exit_intents(
        self,
        trade: LocalTrade,
        row: tuple,
        current_time: datetime,
        row_exit_payload: str | None,
    ) -> str | None:
        exit_intents = json.loads(row_exit_payload) if row_exit_payload else []
        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not sleeves:
            return row_exit_payload

        open_sleeves = [
            sleeve
            for sleeve in sleeves
            if sleeve.get("closed_at") is None and float(sleeve.get("quantity", 0.0)) > 0.0
        ]
        if not open_sleeves:
            return row_exit_payload

        strategies = getattr(self.strategy, "_strategies", None)
        if not isinstance(strategies, dict):
            return row_exit_payload

        for sleeve in open_sleeves:
            strategy_name = sleeve.get("strategy_name")
            if not strategy_name or self._phase1_has_exit_intent(
                exit_intents,
                strategy_name,
                sleeve.get("side"),
            ):
                continue
            sub_strategy = strategies.get(strategy_name)
            if sub_strategy is None:
                continue

            sleeve_open_rate = float(sleeve.get("avg_price") or trade.open_rate)
            current_rate = row[OPEN_IDX]
            if trade.is_short:
                current_profit = (
                    (sleeve_open_rate - current_rate) / sleeve_open_rate
                    if sleeve_open_rate
                    else 0.0
                )
            else:
                current_profit = (
                    (current_rate - sleeve_open_rate) / sleeve_open_rate
                    if sleeve_open_rate
                    else 0.0
                )
            def _get_custom_data(key: str, default=None):
                value = trade.get_custom_data(key)
                return default if value is None else value

            def _set_custom_data(key: str, value):
                trade.set_custom_data(key, value)

            sleeve_trade = SimpleNamespace(
                pair=trade.pair,
                open_date_utc=datetime.fromisoformat(sleeve["opened_at"]),
                open_rate=sleeve_open_rate,
                enter_tag=strategy_name,
                is_short=sleeve.get("side") == "short",
                amount=float(sleeve.get("quantity", 0.0)),
                stake_amount=float(sleeve.get("quantity", 0.0)) * sleeve_open_rate,
                leverage=trade.leverage,
                nr_of_successful_entries=trade.nr_of_successful_entries,
                get_custom_data=_get_custom_data,
                set_custom_data=_set_custom_data,
            )
            result = strategy_safe_wrapper(sub_strategy.custom_exit, default_retval=None)(
                pair=trade.pair,
                trade=sleeve_trade,
                current_time=current_time,
                current_rate=current_rate,
                current_profit=current_profit,
            )
            if not result:
                continue
            exit_intents.append(
                {
                    "strategy_name": strategy_name,
                    "pair": trade.pair,
                    "side": sleeve["side"],
                    "action": "close",
                    "exit_tag": str(result),
                    "timestamp": current_time.isoformat(),
                }
            )

        if not exit_intents:
            return None
        return json.dumps(exit_intents, separators=(",", ":"))

    @staticmethod
    def _phase1_has_exit_intent(
        exit_intents: list[dict],
        strategy_name: str,
        side: str | None,
    ) -> bool:
        return any(
            intent.get("strategy_name") == strategy_name
            and intent.get("side") == side
            and intent.get("action") in ("close", "reduce")
            for intent in exit_intents
        )

    def _check_phase1_owner_exit(
        self,
        trade: LocalTrade,
        row: tuple,
        current_time: datetime,
        exit_: ExitCheckTuple,
    ) -> LocalTrade | None:
        if exit_.exit_type not in (ExitType.CUSTOM_EXIT, ExitType.EXIT_SIGNAL):
            return None
        exit_plan = self._build_phase1_owner_exit_plan(trade)
        if not exit_plan:
            return None
        trade.set_custom_data("phase1_pending_exit_plan", exit_plan)
        trade.exit_reason = exit_plan["exit_reason"]
        return self._exit_trade(
            trade,
            row,
            row[OPEN_IDX],
            exit_plan["exit_amount"],
            exit_plan["exit_reason"],
        )

    @staticmethod
    def _build_phase1_owner_exit_plan(trade: LocalTrade) -> dict | None:
        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not sleeves:
            return None
        side = "short" if trade.is_short else "long"
        open_sleeves = [
            sleeve
            for sleeve in sleeves
            if sleeve.get("side") == side
            and sleeve.get("closed_at") is None
            and float(sleeve.get("quantity", 0.0)) > 0.0
        ]
        if len(open_sleeves) <= 1:
            return None
        owner_strategy_name = (trade.enter_tag or "").split("|", 1)[0]
        if not owner_strategy_name:
            return None
        owner_sleeve_exits = []
        for sleeve in open_sleeves:
            if sleeve.get("strategy_name") != owner_strategy_name:
                continue
            owner_sleeve_exits.append(
                {
                    "sleeve_id": sleeve["sleeve_id"],
                    "quantity": float(sleeve["quantity"]),
                    "quantity_units": float(sleeve.get("quantity_units", sleeve["quantity"])),
                    "close": True,
                }
            )
        if not owner_sleeve_exits:
            return None
        exit_amount = sum(float(item["quantity"]) for item in owner_sleeve_exits)
        if exit_amount <= 0.0:
            return None
        return {
            "strategy_names": [owner_strategy_name],
            "sleeve_ids": [item["sleeve_id"] for item in owner_sleeve_exits],
            "sleeve_exits": owner_sleeve_exits,
            "exit_amount": exit_amount,
            "exit_reason": f"phase1_sleeve_exit:{owner_strategy_name}",
        }

    @staticmethod
    def _build_phase1_net_exit_plan(  # noqa: C901
        trade: LocalTrade,
        phase1_netting_exit_intents: str | None,
    ) -> dict | None:
        if not phase1_netting_exit_intents:
            return None
        sleeves = trade.get_custom_data("phase1_sleeves") or []
        if not sleeves:
            return None
        exit_intents = json.loads(phase1_netting_exit_intents)
        if not exit_intents:
            return None
        side = "short" if trade.is_short else "long"
        matching_intents = [
            intent
            for intent in exit_intents
            if intent.get("side") == side and intent.get("action") in ("close", "reduce")
        ]
        if not matching_intents:
            return None

        sleeve_exits: list[dict] = []
        for intent in matching_intents:
            matching_sleeves = [
                sleeve
                for sleeve in sleeves
                if sleeve["strategy_name"] == intent["strategy_name"]
                and sleeve["side"] == side
                and sleeve["closed_at"] is None
                and float(sleeve["quantity"]) > 0
            ]
            if not matching_sleeves:
                continue
            if intent.get("action") == "close":
                for sleeve in matching_sleeves:
                    sleeve_exits.append(
                        {
                            "sleeve_id": sleeve["sleeve_id"],
                            "quantity": float(sleeve["quantity"]),
                            "quantity_units": float(
                                sleeve.get("quantity_units", sleeve["quantity"])
                            ),
                            "close": True,
                        }
                    )
                continue

            remaining_units = float(intent.get("quantity") or 0.0)
            if remaining_units <= 0.0:
                continue
            for sleeve in matching_sleeves:
                available_units = float(sleeve.get("quantity_units", sleeve["quantity"]))
                if available_units <= 0.0:
                    continue
                reducing_units = min(remaining_units, available_units)
                actual_qty = float(sleeve["quantity"]) * reducing_units / available_units
                sleeve_exits.append(
                    {
                        "sleeve_id": sleeve["sleeve_id"],
                        "quantity": actual_qty,
                        "quantity_units": reducing_units,
                        "close": reducing_units >= available_units,
                    }
                )
                remaining_units -= reducing_units
                if remaining_units <= 0.0:
                    break
            if remaining_units > 0.0:
                return None

        if not sleeve_exits:
            return None
        sleeve_exit_by_id: dict[str, dict] = {}
        for item in sleeve_exits:
            existing = sleeve_exit_by_id.get(item["sleeve_id"])
            if existing is None:
                sleeve_exit_by_id[item["sleeve_id"]] = dict(item)
                continue
            existing["quantity"] += item["quantity"]
            existing["quantity_units"] += item["quantity_units"]
            existing["close"] = existing["close"] or item["close"]
        sleeve_exits = list(sleeve_exit_by_id.values())

        open_sleeves = [
            sleeve
            for sleeve in sleeves
            if sleeve["side"] == side
            and sleeve["closed_at"] is None
            and float(sleeve["quantity"]) > 0
        ]
        full_open_close = len(sleeve_exits) == len(open_sleeves) and all(
            item["close"] for item in sleeve_exits
        )
        if full_open_close:
            exit_amount = trade.amount
        else:
            exit_amount = sum(float(item["quantity"]) for item in sleeve_exits)
        if exit_amount <= 0:
            return None
        strategy_name_by_sleeve_id = {
            sleeve["sleeve_id"]: sleeve["strategy_name"] for sleeve in open_sleeves
        }
        strategy_names = sorted(
            {
                strategy_name_by_sleeve_id[item["sleeve_id"]]
                for item in sleeve_exits
                if item["sleeve_id"] in strategy_name_by_sleeve_id
            }
        )
        only_full_close = all(item["close"] for item in sleeve_exits)
        exit_reason_prefix = "phase1_sleeve_exit" if only_full_close else "phase1_sleeve_reduce"
        return {
            "strategy_names": strategy_names,
            "sleeve_ids": [item["sleeve_id"] for item in sleeve_exits],
            "sleeve_exits": sleeve_exits,
            "exit_amount": exit_amount,
            "exit_reason": f"{exit_reason_prefix}:{','.join(strategy_names)}",
        }

    def _run_funding_fees(self, trade: LocalTrade, current_time: datetime, force: bool = False):
        """
        Calculate funding fees if necessary and add them to the trade.
        """
        if self.trading_mode == TradingMode.FUTURES:
            if force or (current_time.timestamp() % self.funding_fee_timeframe_secs) == 0:
                # Funding fee interval.
                trade.set_funding_fees(
                    self.exchange.calculate_funding_fees(
                        self.futures_data[trade.pair],
                        amount=trade.amount,
                        is_short=trade.is_short,
                        open_date=trade.date_last_filled_utc,
                        close_date=current_time,
                    )
                )

    def get_valid_entry_price_and_stake(
        self,
        pair: str,
        row: tuple,
        propose_rate: float,
        stake_amount: float,
        direction: LongShort,
        current_time: datetime,
        entry_tag: str | None,
        trade: LocalTrade | None,
        order_type: str,
        price_precision: float | None,
        precision_mode_price: int,
    ) -> tuple[float, float, float, float]:
        if order_type == "limit":
            new_rate = strategy_safe_wrapper(
                self.strategy.custom_entry_price, default_retval=propose_rate
            )(
                pair=pair,
                trade=trade,  # type: ignore[arg-type]
                current_time=current_time,
                proposed_rate=propose_rate,
                entry_tag=entry_tag,
                side=direction,
            )  # default value is the open rate
            # We can't place orders higher than current high (otherwise it'd be a stop limit entry)
            # which freqtrade does not support in live.
            if new_rate is not None and new_rate != propose_rate:
                propose_rate = price_to_precision(new_rate, price_precision, precision_mode_price)
            if direction == "short":
                propose_rate = max(propose_rate, row[LOW_IDX])
            else:
                propose_rate = min(propose_rate, row[HIGH_IDX])

        pos_adjust = trade is not None
        leverage = trade.leverage if trade else 1.0
        if not pos_adjust:
            try:
                stake_amount = self.wallets.get_trade_stake_amount(
                    pair, self.strategy.max_open_trades, update=False
                )
            except DependencyException:
                return 0, 0, 0, 0

            max_leverage = self.exchange.get_max_leverage(pair, stake_amount)
            leverage = (
                strategy_safe_wrapper(self.strategy.leverage, default_retval=1.0)(
                    pair=pair,
                    current_time=current_time,
                    current_rate=row[OPEN_IDX],
                    proposed_leverage=1.0,
                    max_leverage=max_leverage,
                    side=direction,
                    entry_tag=entry_tag,
                )
                if self.trading_mode != TradingMode.SPOT
                else 1.0
            )
            # Cap leverage between 1.0 and max_leverage.
            leverage = min(max(leverage, 1.0), max_leverage)

        min_stake_amount = (
            self.exchange.get_min_pair_stake_amount(
                pair, propose_rate, -0.05 if not pos_adjust else 0.0, leverage=leverage
            )
            or 0
        )
        max_stake_amount = self.exchange.get_max_pair_stake_amount(
            pair, propose_rate, leverage=leverage
        )
        stake_available = self.wallets.get_available_stake_amount()

        if not pos_adjust:
            stake_amount = strategy_safe_wrapper(
                self.strategy.custom_stake_amount, default_retval=stake_amount
            )(
                pair=pair,
                current_time=current_time,
                current_rate=propose_rate,
                proposed_stake=stake_amount,
                min_stake=min_stake_amount,
                max_stake=min(stake_available, max_stake_amount),
                leverage=leverage,
                entry_tag=entry_tag,
                side=direction,
            )

        stake_amount_val = self.wallets.validate_stake_amount(
            pair=pair,
            stake_amount=stake_amount,
            min_stake_amount=min_stake_amount,
            max_stake_amount=max_stake_amount,
            trade_amount=trade.stake_amount if trade else None,
        )

        return propose_rate, stake_amount_val, leverage, min_stake_amount

    def _enter_trade(
        self,
        pair: str,
        row: tuple,
        direction: LongShort,
        stake_amount: float | None = None,
        trade: LocalTrade | None = None,
        requested_rate: float | None = None,
        requested_stake: float | None = None,
        entry_tag1: str | None = None,
    ) -> LocalTrade | None:
        """
        :param trade: Trade to adjust - initial entry if None
        :param requested_rate: Adjusted entry rate
        :param requested_stake: Stake amount for adjusted orders (`adjust_entry_price`).
        """

        current_time = row[DATE_IDX].to_pydatetime()
        entry_tag = entry_tag1 or (row[ENTER_TAG_IDX] if len(row) >= ENTER_TAG_IDX + 1 else None)
        phase1_netting_intents = (
            row[PHASE1_NETTING_INTENTS_IDX]
            if len(row) >= PHASE1_NETTING_INTENTS_IDX + 1
            else None
        )
        phase1_netting_exit_intents = (
            row[PHASE1_NETTING_EXIT_INTENTS_IDX]
            if len(row) >= PHASE1_NETTING_EXIT_INTENTS_IDX + 1
            else None
        )
        phase1_plan = self._build_phase1_net_entry_plan(
            phase1_netting_intents,
            direction,
        )
        # let's call the custom entry price, using the open price as default price
        order_type = self.strategy.order_types["entry"]
        pos_adjust = trade is not None and requested_rate is None

        stake_amount_ = stake_amount or (trade.stake_amount if trade else 0.0)
        precision_price, precision_mode_price = self.get_pair_precision(pair, current_time)

        propose_rate, stake_amount, leverage, min_stake_amount = (
            self.get_valid_entry_price_and_stake(
                pair,
                row,
                row[OPEN_IDX],
                stake_amount_,
                direction,
                current_time,
                entry_tag,
                trade,
                order_type,
                precision_price,
                precision_mode_price,
            )
        )

        # replace proposed rate if another rate was requested
        propose_rate = requested_rate if requested_rate else propose_rate
        stake_amount = requested_stake if requested_stake else stake_amount
        if (
            trade is None
            and phase1_plan
            and phase1_plan["net_quantity_delta"] > 0
            and Backtesting._phase1_should_scale_entry_stake(
                self.strategy,
                pair,
                current_time,
                entry_tag,
                direction,
            )
        ):
            stake_amount = stake_amount * phase1_plan["net_quantity_delta"]

        if not stake_amount:
            # In case of pos adjust, still return the original trade
            # If not pos adjust, trade is None
            return trade
        time_in_force = self.strategy.order_time_in_force["entry"]

        if stake_amount and (not min_stake_amount or stake_amount >= min_stake_amount):
            self.order_id_counter += 1
            base_currency = self.exchange.get_pair_base_currency(pair)
            amount_p = (stake_amount / propose_rate) * leverage

            contract_size = self.exchange.get_contract_size(pair)
            precision_amount = self.exchange.get_precision_amount(pair)
            amount = amount_to_contract_precision(
                amount_p, precision_amount, self.precision_mode, contract_size
            )
            if not amount:
                # No amount left after truncating to precision.
                return trade
            # Backcalculate actual stake amount.
            stake_amount = amount * propose_rate / leverage

            if not pos_adjust:
                # Confirm trade entry:
                if not strategy_safe_wrapper(
                    self.strategy.confirm_trade_entry, default_retval=True
                )(
                    pair=pair,
                    order_type=order_type,
                    amount=amount,
                    rate=propose_rate,
                    time_in_force=time_in_force,
                    current_time=current_time,
                    entry_tag=entry_tag,
                    side=direction,
                ):
                    return trade

            is_short = direction == "short"
            # Necessary for Margin trading. Disabled until support is enabled.
            # interest_rate = self.exchange.get_interest_rate()

            if trade is None:
                # Enter trade
                self.trade_id_counter += 1
                trade = LocalTrade(
                    id=self.trade_id_counter,
                    pair=pair,
                    base_currency=base_currency,
                    stake_currency=self.config["stake_currency"],
                    open_rate=propose_rate,
                    open_rate_requested=propose_rate,
                    open_date=current_time,
                    stake_amount=stake_amount,
                    amount=0,
                    amount_requested=amount,
                    fee_open=self.fee,
                    fee_close=self.fee,
                    is_open=True,
                    enter_tag=entry_tag,
                    timeframe=self.timeframe_min,
                    exchange=self._exchange_name,
                    is_short=is_short,
                    trading_mode=self.trading_mode,
                    leverage=leverage,
                    # interest_rate=interest_rate,
                    amount_precision=precision_amount,
                    price_precision=precision_price,
                    precision_mode=self.precision_mode,
                    precision_mode_price=precision_mode_price,
                    contract_size=contract_size,
                    orders=[],
                )
                LocalTrade.add_bt_trade(trade)
                self._attach_phase1_trade_metadata(
                    trade,
                    phase1_netting_intents,
                    phase1_plan,
                    amount,
                    phase1_netting_exit_intents,
                    current_time,
                )
            elif self.handle_similar_order(
                trade, propose_rate, amount, trade.entry_side, current_time
            ):
                return None

            trade.adjust_stop_loss(trade.open_rate, self.strategy.stoploss, initial=True)

            order = Order(
                id=self.order_id_counter,
                ft_trade_id=trade.id,
                ft_is_open=True,
                ft_pair=trade.pair,
                order_id=str(self.order_id_counter),
                symbol=trade.pair,
                ft_order_side=trade.entry_side,
                side=trade.entry_side,
                order_type=order_type,
                status="open",
                order_date=current_time,
                order_filled_date=current_time,
                order_update_date=current_time,
                ft_price=propose_rate,
                price=propose_rate,
                average=propose_rate,
                amount=amount,
                filled=0,
                remaining=amount,
                cost=amount * propose_rate * (1 + self.fee),
                ft_order_tag=entry_tag,
            )
            order._trade_bt = trade
            trade.orders.append(order)
            self._try_close_open_order(order, trade, current_time, row)
            trade.recalc_trade_from_orders()
            if (
                pos_adjust
                and phase1_plan is None
                and entry_tag
                and order.safe_filled > 0.0
            ):
                strategy_name = entry_tag.split("|", 1)[0]
                if strategy_name:
                    Backtesting._apply_phase1_entry_adjustment_metadata(
                        trade=trade,
                        strategy_name=strategy_name,
                        side="short" if trade.is_short else "long",
                        added_quantity=order.safe_filled,
                        fill_price=order.safe_price,
                        current_time=current_time,
                    )

        return trade

    @staticmethod
    def _phase1_should_scale_entry_stake(
        strategy: Any,
        pair: str,
        current_time: datetime,
        entry_tag: str | None,
        side: str,
    ) -> bool:
        checker = getattr(strategy, "phase1_entry_stake_is_aggregate", None)
        if not callable(checker):
            return True
        return not bool(
            strategy_safe_wrapper(checker, default_retval=False)(
                pair=pair,
                current_time=current_time,
                entry_tag=entry_tag,
                side=side,
            )
        )

    @staticmethod
    def _attach_phase1_trade_metadata(
        trade: LocalTrade,
        phase1_netting_intents: str | None,
        phase1_plan: dict | None,
        aggregate_amount: float,
        phase1_netting_exit_intents: str | None,
        current_time: datetime,
    ) -> None:
        """Attach experimental contributor intent payload to backtest trades."""
        if not phase1_netting_intents and not phase1_plan:
            return
        if phase1_netting_intents:
            trade.set_custom_data("phase1_netting_intents", phase1_netting_intents)
        if phase1_plan:
            phase1_plan = Backtesting._materialize_phase1_sleeve_amounts(
                phase1_plan, aggregate_amount
            )
            trade.set_custom_data("phase1_net_plan", phase1_plan)
            trade.set_custom_data("phase1_sleeves", phase1_plan.get("sleeves", []))
            trade.set_custom_data(
                "phase1_net_quantity_delta",
                phase1_plan.get("net_quantity_delta"),
            )
        if phase1_netting_exit_intents:
            trade.set_custom_data(
                "phase1_last_seen_exit_intents",
                phase1_netting_exit_intents,
            )
        trade.set_custom_data(
            "phase1_netting_initialized_at",
            current_time.isoformat(),
        )

    @staticmethod
    def _materialize_phase1_sleeve_amounts(
        phase1_plan: dict,
        aggregate_amount: float,
    ) -> dict:
        """Translate contributor units into actual trade amounts."""
        net_quantity_delta = float(phase1_plan["net_quantity_delta"])
        updated = dict(phase1_plan)
        sleeves = []
        for sleeve in phase1_plan["sleeves"]:
            sleeve_copy = dict(sleeve)
            unit_quantity = float(sleeve_copy["quantity"])
            sleeve_copy["quantity_units"] = unit_quantity
            sleeve_copy["quantity"] = aggregate_amount * unit_quantity / net_quantity_delta
            sleeves.append(sleeve_copy)
        updated["sleeves"] = sleeves
        return updated

    @staticmethod
    def _build_phase1_net_entry_plan(
        phase1_netting_intents: str | None,
        direction: LongShort,
    ) -> dict | None:
        """Aggregate same-direction contributor intents into one entry plan."""
        if not phase1_netting_intents:
            return None
        intents = json.loads(phase1_netting_intents)
        if not intents:
            return None
        side = "long" if direction == "long" else "short"
        if any(intent.get("side") != side for intent in intents):
            return None

        net_quantity_delta = 0.0
        sleeves: list[dict] = []
        strategy_names: list[str] = []
        for intent in intents:
            quantity = float(intent["quantity"])
            action = intent["action"]
            if action in ("open", "increase"):
                net_quantity_delta += quantity
            else:
                net_quantity_delta -= quantity
            strategy_name = intent["strategy_name"]
            strategy_names.append(strategy_name)
            sleeves.append(
                {
                    "sleeve_id": intent["sleeve_id"],
                    "strategy_name": strategy_name,
                    "pair": intent["pair"],
                    "side": intent["side"],
                    "quantity": quantity,
                    "avg_price": float(intent["price"]),
                    "opened_at": intent["timestamp"],
                    "updated_at": intent["timestamp"],
                    "realized_pnl": 0.0,
                    "closed_at": None,
                }
            )
        if net_quantity_delta <= 0:
            return None
        return {
            "direction": direction,
            "contributor_count": len(sleeves),
            "net_quantity_delta": net_quantity_delta,
            "strategy_names": strategy_names,
            "sleeves": sleeves,
        }

    def handle_left_open(
        self, open_trades: dict[str, list[LocalTrade]], data: dict[str, list[tuple]]
    ) -> None:
        """
        Handling of left open trades at the end of backtesting
        """
        for pair in open_trades.keys():
            for trade in list(open_trades[pair]):
                if (
                    trade.has_open_orders and trade.nr_of_successful_entries == 0
                ) or not trade.has_open_position:
                    # Ignore trade if entry-order did not fill yet
                    LocalTrade.remove_bt_trade(trade)
                    continue

                exit_row = data[pair][-1]
                self._exit_trade(
                    trade, exit_row, exit_row[OPEN_IDX], trade.amount, ExitType.FORCE_EXIT.value
                )
                trade.exit_reason = ExitType.FORCE_EXIT.value
                self._process_exit_order(
                    trade.orders[-1], trade, exit_row[DATE_IDX].to_pydatetime(), exit_row, pair
                )

    def trade_slot_available(self, open_trade_count: int) -> bool:
        # Always allow trades when max_open_trades is enabled.
        max_open_trades: IntOrInf = self.strategy.max_open_trades
        if max_open_trades <= 0 or open_trade_count < max_open_trades:
            return True
        # Rejected trade
        self.rejected_trades += 1
        return False

    def check_for_trade_entry(self, row) -> LongShort | None:
        enter_long = row[LONG_IDX] == 1
        exit_long = row[ELONG_IDX] == 1
        enter_short = self._can_short and row[SHORT_IDX] == 1
        exit_short = self._can_short and row[ESHORT_IDX] == 1

        if enter_long == 1 and not any([exit_long, enter_short]):
            # Long
            return "long"
        if enter_short == 1 and not any([exit_short, enter_long]):
            # Short
            return "short"
        return None

    def run_protections(self, pair: str, current_time: datetime, side: LongShort):
        if self.enable_protections:
            self.protections.stop_per_pair(pair, current_time, side)
            self.protections.global_stop(current_time, side)

    def manage_open_orders(self, trade: LocalTrade, current_time: datetime, row: tuple) -> bool:
        """
        Check if any open order needs to be cancelled or replaced.
        Returns True if the trade should be deleted.
        """
        for order in [o for o in trade.orders if o.ft_is_open]:
            oc = self.check_order_cancel(trade, order, current_time)
            if oc:
                # delete trade due to order timeout
                return True
            elif oc is None and self.check_order_replace(trade, order, current_time, row):
                # delete trade due to user request
                self.canceled_trade_entries += 1
                return True
        # default maintain trade
        return False

    def cancel_open_orders(self, trade: LocalTrade, current_time: datetime):
        """
        Cancel all open orders for the given trade.
        """
        for order in [o for o in trade.orders if o.ft_is_open]:
            if order.side == trade.entry_side:
                self.canceled_entry_orders += 1
            elif order.side == trade.exit_side:
                self.canceled_exit_orders += 1
            # canceled orders are removed from the trade
            del trade.orders[trade.orders.index(order)]

    def handle_similar_order(
        self, trade: LocalTrade, price: float, amount: float, side: str, current_time: datetime
    ) -> bool:
        """
        Handle similar order for the given trade.
        """
        if trade.has_open_orders:
            oo = trade.select_order(side, True)
            if oo:
                if (price == oo.price) and (side == oo.side) and (amount == oo.amount):
                    # logger.info(
                    #     f"A similar open order was found for {trade.pair}. "
                    #     f"Keeping existing {trade.exit_side} order. {price=},  {amount=}"
                    # )
                    return True
            self.cancel_open_orders(trade, current_time)

        return False

    def check_order_cancel(
        self, trade: LocalTrade, order: Order, current_time: datetime
    ) -> bool | None:
        """
        Check if current analyzed order has to be canceled.
        Returns True if the trade should be Deleted (initial order was canceled),
                False if it's Canceled
                None if the order is still active.
        """
        timedout = self.strategy.ft_check_timed_out(
            trade,  # type: ignore[arg-type]
            order,
            current_time,
        )
        if timedout:
            if order.side == trade.entry_side:
                self.timedout_entry_orders += 1
                if trade.nr_of_successful_entries == 0:
                    # Remove trade due to entry timeout expiration.
                    return True
                else:
                    # Close additional entry order
                    del trade.orders[trade.orders.index(order)]
                    return False
            if order.side == trade.exit_side:
                self.timedout_exit_orders += 1
                # Close exit order and retry exiting on next signal.
                del trade.orders[trade.orders.index(order)]
                return False
        return None

    def check_order_replace(
        self, trade: LocalTrade, order: Order, current_time, row: tuple
    ) -> bool:
        """
        Check if current analyzed entry order has to be replaced and do so.
        If user requested cancellation and there are no filled orders in the trade will
        instruct caller to delete the trade.
        Returns True if the trade should be deleted.
        """
        # only check on new candles for open entry orders
        if current_time > order.order_date_utc:
            is_entry = order.side == trade.entry_side
            requested_rate = strategy_safe_wrapper(
                self.strategy.adjust_order_price, default_retval=order.ft_price
            )(
                trade=trade,  # type: ignore[arg-type]
                order=order,
                pair=trade.pair,
                current_time=current_time,
                proposed_rate=row[OPEN_IDX],
                current_order_rate=order.ft_price,
                entry_tag=trade.enter_tag,
                side=trade.trade_direction,
                is_entry=is_entry,
            )  # default value is current order price

            # cancel existing order whenever a new rate is requested (or None)
            if requested_rate == order.ft_price:
                # assumption: there can't be multiple open entry orders at any given time
                return False
            else:
                del trade.orders[trade.orders.index(order)]
                if is_entry:
                    self.canceled_entry_orders += 1
                else:
                    self.canceled_exit_orders += 1

            # place new order if result was not None
            if requested_rate:
                if is_entry:
                    self._enter_trade(
                        pair=trade.pair,
                        row=row,
                        trade=trade,
                        requested_rate=requested_rate,
                        requested_stake=(order.safe_remaining * order.ft_price / trade.leverage),
                        direction="short" if trade.is_short else "long",
                    )
                    self.replaced_entry_orders += 1
                else:
                    self._exit_trade(
                        trade=trade,
                        sell_row=row,
                        close_rate=requested_rate,
                        amount=order.safe_remaining,
                        exit_reason=order.ft_order_tag,
                    )
                    self.replaced_exit_orders += 1
                # Delete trade if no successful entries happened (if placing the new order failed)
                if not trade.has_open_orders and is_entry and trade.nr_of_successful_entries == 0:
                    return True
            else:
                # assumption: there can't be multiple open entry orders at any given time
                return trade.nr_of_successful_entries == 0
        return False

    def validate_row(
        self, data: dict, pair: str, row_index: int, current_time: datetime
    ) -> tuple | None:
        try:
            # Row is treated as "current incomplete candle".
            # entry / exit signals are shifted by 1 to compensate for this.
            row = data[pair][row_index]
        except IndexError:
            # missing Data for one pair at the end.
            # Warnings for this are shown during data loading
            return None

        # Waits until the time-counter reaches the start of the data for this pair.
        if row[DATE_IDX] > current_time:
            return None
        return row

    def _collate_rejected(self, pair, row):
        """
        Temporarily store rejected signal information for downstream use in backtesting_analysis
        """
        # It could be fun to enable hyperopt mode to write
        # a loss function to reduce rejected signals
        if (
            self.config.get("export", "none") == "signals"
            and self.dataprovider.runmode == RunMode.BACKTEST
        ):
            if pair not in self.rejected_dict:
                self.rejected_dict[pair] = []
            self.rejected_dict[pair].append([row[DATE_IDX], row[ENTER_TAG_IDX]])

    def backtest_loop(
        self,
        row: tuple,
        pair: str,
        current_time: datetime,
        trade_dir: LongShort | None,
        can_enter: bool,
    ) -> LongShort | None:
        """
        NOTE: This method is used by Hyperopt at each iteration. Please keep it optimized.

        Backtesting processing for one candle/pair.
        """
        exiting_dir: LongShort | None = None
        if not self._position_stacking and len(LocalTrade.bt_trades_open_pp[pair]) > 0:
            # position_stacking not supported for now.
            exiting_dir = "short" if LocalTrade.bt_trades_open_pp[pair][0].is_short else "long"

        for t in list(LocalTrade.bt_trades_open_pp[pair]):
            # 1. Manage currently open orders of active trades
            if self.manage_open_orders(t, current_time, row):
                # Remove trade (initial open order never filled)
                LocalTrade.remove_bt_trade(t)
                self.wallets.update()

        # 2. Process entries.
        # without positionstacking, we can only have one open trade per pair.
        # max_open_trades must be respected
        # don't open on the last row
        # We only open trades on the main candle, not on detail candles
        if (
            can_enter
            and trade_dir is not None
            and (self._position_stacking or len(LocalTrade.bt_trades_open_pp[pair]) == 0)
            and not PairLocks.is_pair_locked(pair, row[DATE_IDX], trade_dir)
        ):
            if self.trade_slot_available(LocalTrade.bt_open_open_trade_count):
                trade = self._enter_trade(pair, row, trade_dir)
                if trade:
                    self.wallets.update()
            else:
                self._collate_rejected(pair, row)

        for trade in list(LocalTrade.bt_trades_open_pp[pair]):
            # 3. Process entry orders.
            order = trade.select_order(trade.entry_side, is_open=True)
            if self._try_close_open_order(order, trade, current_time, row):
                self.wallets.update()

            # 4. Create exit orders (if any)
            if trade.has_open_position:
                self._check_trade_exit(trade, row, current_time)  # Place exit order if necessary

            # 5. Process exit orders.
            order = trade.select_order(trade.exit_side, is_open=True)
            if order:
                self._process_exit_order(order, trade, current_time, row, pair)

        if exiting_dir and len(LocalTrade.bt_trades_open_pp[pair]) == 0:
            return exiting_dir
        return None

    def get_detail_data(self, pair: str, row: tuple) -> list[tuple] | None:
        """
        Spread into detail data
        """
        current_detail_time: datetime = row[DATE_IDX].to_pydatetime()
        exit_candle_end = current_detail_time + self.timeframe_td
        detail_data = self.detail_data[pair]
        detail_data = detail_data.loc[
            (detail_data["date"] >= current_detail_time) & (detail_data["date"] < exit_candle_end)
        ].copy()

        if len(detail_data) == 0:
            return None
        detail_data.loc[:, "enter_long"] = row[LONG_IDX]
        detail_data.loc[:, "exit_long"] = row[ELONG_IDX]
        detail_data.loc[:, "enter_short"] = row[SHORT_IDX]
        detail_data.loc[:, "exit_short"] = row[ESHORT_IDX]
        detail_data.loc[:, "enter_tag"] = row[ENTER_TAG_IDX]
        detail_data.loc[:, "exit_tag"] = row[EXIT_TAG_IDX]
        detail_data.loc[:, "phase1_netting_intents"] = row[PHASE1_NETTING_INTENTS_IDX]
        detail_data.loc[:, "phase1_netting_exit_intents"] = row[PHASE1_NETTING_EXIT_INTENTS_IDX]
        return detail_data[HEADERS].values.tolist()

    def _time_generator(self, start_date: datetime, end_date: datetime):
        current_time = start_date + self.timeframe_td
        while current_time <= end_date:
            yield current_time
            current_time += self.timeframe_td

    def _time_generator_det(self, start_date: datetime, end_date: datetime):
        """
        Loop for each detail candle.
        Yields only the start date if no detail timeframe is set.
        """
        if not self.timeframe_detail_td:
            yield start_date, True, False, 0
            return

        current_time = start_date
        i = 0
        while current_time <= end_date:
            yield current_time, i == 0, True, i
            i += 1
            current_time += self.timeframe_detail_td

    def _time_pair_generator_det(self, current_time: datetime, pairs: list[str]):
        for current_time_det, is_first, has_detail, idx in self._time_generator_det(
            current_time, current_time + self.timeframe_td
        ):
            # Pairs that have open trades should be processed first
            new_pairlist = list(dict.fromkeys([t.pair for t in LocalTrade.bt_trades_open] + pairs))
            for pair in new_pairlist:
                yield current_time_det, is_first, has_detail, idx, pair

    def time_pair_generator(  # noqa: C901
        self,
        start_date: datetime,
        end_date: datetime,
        pairs: list[str],
        data: dict[str, list[tuple]],
    ):
        """
        Backtest time and pair generator
        :returns: generator of (current_time, pair, row, is_last_row, trade_dir)
            where is_last_row is a boolean indicating if this is the data end date.
        """
        current_time = start_date + self.timeframe_td
        self.progress.init_step(
            BacktestState.BACKTEST, int((end_date - start_date) / self.timeframe_td)
        )
        # Indexes per pair, so some pairs are allowed to have a missing start.
        indexes: dict = defaultdict(int)
        _last_pairlist_date = None

        for current_time in self._time_generator(start_date, end_date):
            # Loop for each main candle.
            self.check_abort()

            if self.dynamic_pairlist and self.pairlists:
                _current_date = current_time.date()
                if _current_date != _last_pairlist_date:
                    self.pairlists.refresh_pairlist(
                        pairs=self.available_pairs, current_time=current_time
                    )
                    pairs = self.pairlists.whitelist
                    _last_pairlist_date = _current_date

            # Reset open trade count for this candle
            # Critical to avoid exceeding max_open_trades in backtesting
            # when timeframe-detail is used and trades close within the opening candle.
            strategy_safe_wrapper(self.strategy.bot_loop_start, supress_error=True)(
                current_time=current_time
            )
            pair_detail_cache: dict[str, list[tuple]] = {}
            pair_tradedir_cache: dict[str, LongShort | None] = {}
            pairs_with_open_trades = [t.pair for t in LocalTrade.bt_trades_open]

            for current_time_det, is_first, has_detail, idx, pair in self._time_pair_generator_det(
                current_time, pairs
            ):
                # Loop for each detail candle (if necessary) and pair
                # Yields only the main date if no detail timeframe is set.

                # Pairs that have open trades should be processed first
                trade_dir: LongShort | None = None
                if is_first:
                    # Main candle
                    row_index = indexes[pair]
                    row = self.validate_row(data, pair, row_index, current_time)
                    if not row:
                        continue

                    row_index += 1
                    indexes[pair] = row_index
                    is_last_row = current_time == end_date
                    self.dataprovider._set_dataframe_max_index(
                        pair, self.required_startup + row_index
                    )
                    trade_dir = self.check_for_trade_entry(row)
                    pair_tradedir_cache[pair] = trade_dir

                else:
                    # Detail candle - from cache.
                    detail_data = pair_detail_cache.get(pair)
                    if detail_data is None or len(detail_data) <= idx:
                        # logger.info(f"skipping {pair}, {current_time_det}, {trade_dir}")
                        continue
                    row = detail_data[idx]
                    trade_dir = pair_tradedir_cache.get(pair)

                    if self.strategy.ignore_expired_candle(
                        current_time - self.timeframe_td,  # last closed candle is 1 timeframe away.
                        current_time_det,
                        self.timeframe_secs,
                        trade_dir is not None,
                    ):
                        # Ignore late entries eventually
                        trade_dir = None

                self.dataprovider._set_dataframe_max_date(current_time_det)

                pair_has_open_trades = len(LocalTrade.bt_trades_open_pp[pair]) > 0
                if pair in pairs_with_open_trades and not pair_has_open_trades:
                    # Pair has had open trades which closed in the current main candle.
                    # Skip this pair for this timeframe
                    continue
                if pair_has_open_trades and pair not in pairs_with_open_trades:
                    # auto-lock for pairs that have open trades
                    # Necessary for detail - to capture trades that open and close within
                    # the same main candle
                    pairs_with_open_trades.append(pair)

                if (
                    is_first
                    and (trade_dir is not None or pair_has_open_trades)
                    and has_detail
                    and pair not in pair_detail_cache
                    and pair in self.detail_data
                    and row
                ):
                    # Spread candle into detail timeframe and cache that -
                    # only once per main candle
                    # and only if we can expect activity.
                    pair_detail = self.get_detail_data(pair, row)
                    if pair_detail is not None:
                        pair_detail_cache[pair] = pair_detail
                        row = pair_detail_cache[pair][idx]

                is_last_row = current_time_det == end_date

                yield current_time_det, pair, row, is_last_row, trade_dir
            self.progress.increment()

    def backtest(
        self, processed: dict, start_date: datetime, end_date: datetime
    ) -> BacktestContentTypeIcomplete:
        """
        Implement backtesting functionality

        NOTE: This method is used by Hyperopt at each iteration. Please keep it optimized.
        Of course try to not have ugly code. By some accessor are sometime slower than functions.
        Avoid extensive logging in this method and functions it calls.

        :param processed: a processed dictionary with format {pair, data}, which gets cleared to
        optimize memory usage!
        :param start_date: backtesting timerange start datetime
        :param end_date: backtesting timerange end datetime
        :return: DataFrame with trades (results of backtesting)
        """
        self.prepare_backtest(self.enable_protections)
        # Ensure wallets are up-to-date (important for --strategy-list)
        self.wallets.update()
        # Use dict of lists with data for performance
        # (looping lists is a lot faster than pandas DataFrames)
        data: dict = self._get_ohlcv_as_lists(processed)

        # Loop timerange and get candle for each pair at that point in time
        for (
            current_time,
            pair,
            row,
            is_last_row,
            trade_dir,
        ) in self.time_pair_generator(start_date, end_date, list(data.keys()), data):
            if not self._can_short or trade_dir is None:
                # No need to reverse position if shorting is disabled or there's no new signal
                self.backtest_loop(row, pair, current_time, trade_dir, not is_last_row)
            else:
                # Conditionally call backtest_loop a 2nd time if shorting is enabled,
                # a position closed and a new signal in the other direction is available.

                for _ in (0, 1):
                    a = self.backtest_loop(row, pair, current_time, trade_dir, not is_last_row)
                    if not a or a == trade_dir:
                        # the trade didn't close or position change is in the same direction
                        break

        self.handle_left_open(LocalTrade.bt_trades_open_pp, data=data)
        self.wallets.update()

        results = trade_list_to_dataframe(LocalTrade.bt_trades)
        return {
            "results": results,
            "config": self.strategy.config,
            "locks": PairLocks.get_all_locks(),
            "rejected_signals": self.rejected_trades,
            "timedout_entry_orders": self.timedout_entry_orders,
            "timedout_exit_orders": self.timedout_exit_orders,
            "canceled_trade_entries": self.canceled_trade_entries,
            "canceled_entry_orders": self.canceled_entry_orders,
            "replaced_entry_orders": self.replaced_entry_orders,
            "final_balance": self.wallets.get_total(self.strategy.config["stake_currency"]),
        }

    def backtest_one_strategy(
        self, strat: IStrategy, data: dict[str, DataFrame], timerange: TimeRange
    ):
        self.progress.init_step(BacktestState.ANALYZE, 0)
        strategy_name = strat.get_strategy_name()
        logger.info(f"Running backtesting for Strategy {strategy_name}")
        backtest_start_time = dt_now()
        self._set_strategy(strat)

        # need to reprocess data every time to populate signals
        preprocessed = self.strategy.advise_all_indicators(data)

        # Trim startup period from analyzed dataframe
        # This only used to determine if trimming would result in an empty dataframe
        preprocessed_tmp = trim_dataframes(preprocessed, timerange, self.required_startup)

        if not preprocessed_tmp:
            raise OperationalException("No data left after adjusting for startup candles.")

        # Use preprocessed_tmp for date generation (the trimmed dataframe).
        # Backtesting will re-trim the dataframes after entry/exit signal generation.
        min_date, max_date = history.get_timerange(preprocessed_tmp)
        logger.info(
            f"Backtesting with data from {min_date.strftime(DATETIME_PRINT_FORMAT)} "
            f"up to {max_date.strftime(DATETIME_PRINT_FORMAT)} "
            f"({(max_date - min_date).days} days)."
        )
        # Execute backtest and store results
        results = self.backtest(
            processed=preprocessed,
            start_date=min_date,
            end_date=max_date,
        )
        backtest_end_time = dt_now()
        results.update(
            {
                "run_id": self.run_ids.get(strategy_name, ""),
                "backtest_start_time": int(backtest_start_time.timestamp()),
                "backtest_end_time": int(backtest_end_time.timestamp()),
            }
        )
        self.all_bt_content[strategy_name] = results

        if (
            self.config.get("export", "none") == "signals"
            and self.dataprovider.runmode == RunMode.BACKTEST
        ):
            signals = generate_trade_signal_candles(preprocessed_tmp, results, "open_date")
            rejected = generate_rejected_signals(preprocessed_tmp, self.rejected_dict)
            exited = generate_trade_signal_candles(preprocessed_tmp, results, "close_date")

            self.analysis_results["signals"][strategy_name] = signals
            self.analysis_results["rejected"][strategy_name] = rejected
            self.analysis_results["exited"][strategy_name] = exited

        return min_date, max_date

    def _get_min_cached_backtest_date(self):
        min_backtest_date = None
        backtest_cache_age = self.config.get("backtest_cache", constants.BACKTEST_CACHE_DEFAULT)
        if self.timerange.stopts == 0 or self.timerange.stopdt > dt_now():
            logger.warning("Backtest result caching disabled due to use of open-ended timerange.")
        elif backtest_cache_age == "day":
            min_backtest_date = dt_now() - timedelta(days=1)
        elif backtest_cache_age == "week":
            min_backtest_date = dt_now() - timedelta(weeks=1)
        elif backtest_cache_age == "month":
            min_backtest_date = dt_now() - timedelta(weeks=4)
        return min_backtest_date

    def load_prior_backtest(self):
        self.run_ids = {
            strategy.get_strategy_name(): get_strategy_run_id(strategy)
            for strategy in self.strategylist
        }

        # Load previous result that will be updated incrementally.
        # This can be circumvented in certain instances in combination with downloading more data
        min_backtest_date = self._get_min_cached_backtest_date()
        if min_backtest_date is not None:
            self.results = find_existing_backtest_stats(
                self.config["user_data_dir"] / "backtest_results", self.run_ids, min_backtest_date
            )

    def start(self) -> None:
        """
        Run backtesting end-to-end
        """
        data: dict[str, DataFrame] = {}

        data, timerange = self.load_bt_data()
        logger.info("Dataload complete. Calculating indicators")

        self.load_prior_backtest()

        for strat in self.strategylist:
            if self.results and strat.get_strategy_name() in self.results["strategy"]:
                # When previous result hash matches - reuse that result and skip backtesting.
                logger.info(f"Reusing result of previous backtest for {strat.get_strategy_name()}")
                continue
            min_date, max_date = self.backtest_one_strategy(strat, data, timerange)

        # Update old results with new ones.
        if len(self.all_bt_content) > 0:
            results = generate_backtest_stats(
                data,
                self.all_bt_content,
                min_date=min_date,
                max_date=max_date,
                notes=self.config.get("backtest_notes"),
            )
            if self.results:
                self.results["metadata"].update(results["metadata"])
                self.results["strategy"].update(results["strategy"])
                self.results["strategy_comparison"].extend(results["strategy_comparison"])
            else:
                self.results = results
            dt_appendix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            if self.config.get("export", "none") in ("trades", "signals"):
                combined_res = combined_dataframes_with_rel_mean(data, min_date, max_date)
                store_backtest_results(
                    self.config,
                    self.results,
                    dt_appendix,
                    market_change_data=combined_res,
                    analysis_results=self.analysis_results,
                    strategy_files={s.get_strategy_name(): s.__file__ for s in self.strategylist},
                )

        # Results may be mixed up now. Sort them so they follow --strategy-list order.
        if "strategy_list" in self.config and len(self.results) > 0:
            self.results["strategy_comparison"] = sorted(
                self.results["strategy_comparison"],
                key=lambda c: self.config["strategy_list"].index(c["key"]),
            )
            self.results["strategy"] = dict(
                sorted(
                    self.results["strategy"].items(),
                    key=lambda kv: self.config["strategy_list"].index(kv[0]),
                )
            )

        if len(self.strategylist) > 0:
            # Show backtest results
            show_backtest_results(self.config, self.results)
