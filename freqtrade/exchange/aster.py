"""Aster exchange subclass.

- Initial implementation copy-pasted from Binance subclass
"""

import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import ccxt
from pandas import DataFrame

from freqtrade.constants import DEFAULT_DATAFRAME_COLUMNS
from freqtrade.enums import CandleType, MarginMode, PriceType, TradingMode
from freqtrade.exceptions import DDosProtection, OperationalException, TemporaryError
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange_types import FtHas, Tickers
from freqtrade.exchange.exchange_utils_timeframe import timeframe_to_msecs
from freqtrade.misc import deep_merge_dicts, json_load
from freqtrade.util.datetime_helpers import dt_from_ts, dt_ts


logger = logging.getLogger(__name__)


class Aster(Exchange):

    _ft_has: FtHas = {
        "stoploss_on_exchange": True,
        "stop_price_param": "stopPrice",
        "stop_price_prop": "stopPrice",
        "stoploss_order_types": {"limit": "stop_loss_limit"},
        "order_time_in_force": ["GTC", "FOK", "IOC", "PO"],
        "trades_pagination": "id",
        "trades_pagination_arg": "fromId",
        "trades_has_history": True,
        "l2_limit_range": [5, 10, 20, 50, 100, 500, 1000],
        "ws_enabled": True,
    }

    _ft_has_futures: FtHas = {
        "funding_fee_candle_limit": 1000,
        "stoploss_order_types": {"limit": "stop", "market": "stop_market"},
        "order_time_in_force": ["GTC", "FOK", "IOC"],
        "tickers_have_price": False,
        "floor_leverage": True,
        "stop_price_type_field": "workingType",
        "order_props_in_contracts": ["amount", "cost", "filled", "remaining"],
        "stop_price_type_value_mapping": {
            PriceType.LAST: "CONTRACT_PRICE",
            PriceType.MARK: "MARK_PRICE",
        },
        "ws_enabled": True,
        "proxy_coin_mapping": {
            "ASTERCR": "USDC",
            "ASTUSD": "USDT",
        },
        "uses_leverage_tiers": False,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        # TradingMode.SPOT always supported and not required in this list
        # (TradingMode.MARGIN, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def validate_config(self, config):
        super().validate_config(config)

        # Monkey patch None -> empty dict as needed
        self._api_async.features = {
            "spot": {},
            "swap": {},
            "future": {},
        }

    def get_proxy_coin(self) -> str:
        """
        Get the proxy coin for the given coin
        Falls back to the stake currency if no proxy coin is found
        :return: Proxy coin or stake currency
        """
        if self.margin_mode == MarginMode.CROSS:
            return self._config.get("proxy_coin", self._config["stake_currency"])
        return self._config["stake_currency"]

    def get_tickers(
        self,
        symbols: list[str] | None = None,
        *,
        cached: bool = False,
        market_type: TradingMode | None = None,
    ) -> Tickers:
        tickers = super().get_tickers(symbols=symbols, cached=cached, market_type=market_type)
        if self.trading_mode == TradingMode.FUTURES:
            # Aster's future result has no bid/ask values.
            # Therefore we must fetch that from fetch_bids_asks and combine the two results.
            bidsasks = self.fetch_bids_asks(symbols, cached=cached)
            tickers = deep_merge_dicts(bidsasks, tickers, allow_null_overrides=False)
        return tickers

    # TODO: THis was a like for like replcament using Binance but could not find the specific endpoints
    # @retrier
    # def additional_exchange_init(self) -> None:
    #     """
    #     Additional exchange initialization logic.
    #     .api will be available at this point.
    #     Must be overridden in child methods if required.
    #     """
    #     try:
    #         if self.trading_mode == TradingMode.FUTURES and not self._config["dry_run"]:
    #             # Aster-specific futures initialization
    #             # Note: Replace these API calls with Aster-specific equivalents
    #             position_side = self._api.fapiPrivateGetPositionSideDual()
    #             # self._log_exchange_response("position_side_setting", position_side)
    #             # assets_margin = self._api.fapiPrivateGetMultiAssetsMargin()
    #             # self._log_exchange_response("multi_asset_margin", assets_margin)
    #             msg = ""
    #             # if position_side.get("dualSidePosition") is True:
    #             #     msg += (
    #             #         "\nHedge Mode is not supported by freqtrade. "
    #             #         "Please change 'Position Mode' on your aster futures account."
    #             #     )
    #             # if (
    #             #     assets_margin.get("multiAssetsMargin") is True
    #             #     and self.margin_mode != MarginMode.CROSS
    #             # ):
    #             #     msg += (
    #             #         "\nMulti-Asset Mode is not supported by freqtrade. "
    #             #         "Please change 'Asset Mode' on your aster futures account."
    #             #     )
    #             if msg:
    #                 raise OperationalException(msg)
    #     except ccxt.DDoSProtection as e:
    #         raise DDosProtection(e) from e
    #     except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
    #         raise TemporaryError(
    #             f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
    #         ) from e

    #     except ccxt.BaseError as e:
    #         raise OperationalException(e) from e
    #
    # @retrier
    # def additional_exchange_init(self) -> None:
    #     """
    #     Additional exchange initialization logic.
    #     .api will be available at this point.
    #     Must be overridden in child methods if required.
    #     """
    #     try:
    #         if not self._config["dry_run"]:
    #             if self.trading_mode == TradingMode.FUTURES:
    #                 position_mode = self._api.set_position_mode(False)
    #                 self._log_exchange_response("set_position_mode", position_mode)
    #             is_unified = self._api.is_unified_enabled()
    #             # Returns a tuple of bools, first for margin, second for Account
    #             if is_unified and len(is_unified) > 1 and is_unified[1]:
    #                 self.unified_account = True
    #                 logger.info(
    #                     "Bybit: Unified account. Assuming dedicated subaccount for this bot."
    #                 )
    #             else:
    #                 self.unified_account = False
    #                 logger.info("Bybit: Standard account.")
    #     except ccxt.DDoSProtection as e:
    #         raise DDosProtection(e) from e
    #     except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
    #         raise TemporaryError(
    #             f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
    #         ) from e
    #     except ccxt.BaseError as e:
    #         raise OperationalException(e) from e

    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        is_new_pair: bool = False,
        until_ms: int | None = None,
    ) -> DataFrame:
        """
        Overwrite to introduce "fast new pair" functionality by detecting the pair's listing date
        Does not work for other exchanges, which don't return the earliest data when called with "0"
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        """
        if is_new_pair and candle_type in (CandleType.SPOT, CandleType.FUTURES, CandleType.MARK):
            with self._loop_lock:
                assert self._api_async
                assert self._api_async.features

                x = self.loop.run_until_complete(
                    self._async_get_candle_history(pair, timeframe, candle_type, 0)
                )
            if x and x[3] and x[3][0] and x[3][0][0] > since_ms:
                # Set starting date to first available candle.
                since_ms = x[3][0][0]
                logger.info(
                    f"Candle-data for {pair} available starting with "
                    f"{datetime.fromtimestamp(since_ms // 1000, tz=timezone.utc).isoformat()}."
                )
                if until_ms and since_ms >= until_ms:
                    logger.warning(
                        f"No available candle-data for {pair} before "
                        f"{dt_from_ts(until_ms).isoformat()}"
                    )
                    return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

        # For Aster, we'll use the standard REST API approach
        # Remove the fast download functionality since Aster doesn't have a public data API like Binance
        return super().get_historic_ohlcv(
            pair=pair,
            timeframe=timeframe,
            since_ms=since_ms,
            candle_type=candle_type,
            is_new_pair=is_new_pair,
            until_ms=until_ms,
        )

    def funding_fee_cutoff(self, open_date: datetime):
        """
        Funding fees are only charged at full hours (usually every 4-8h).
        Therefore a trade opening at 10:00:01 will not be charged a funding fee until the next hour.
        On Aster, this cutoff is 15s (adjust if Aster has different timing).
        :param open_date: The open date for a trade
        :return: True if the date falls on a full hour, False otherwise
        """
        return open_date.minute == 0 and open_date.second < 15

    def fetch_funding_rates(self, symbols: list[str] | None = None) -> dict[str, dict[str, float]]:
        """
        Fetch funding rates for the given symbols.
        :param symbols: List of symbols to fetch funding rates for
        :return: Dict of funding rates for the given symbols
        """
        try:
            if self.trading_mode == TradingMode.FUTURES:
                rates = self._api.fetch_funding_rates(symbols)
                return rates
            return {}
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Error in fetch_funding_rates due to {e.__class__.__name__}. Message: {e}"
            ) from e

        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,
        open_trades: list,
    ) -> float | None:
        """
        Important: Must be fetching data from cached values as this is used by backtesting!
        Aster-specific liquidation price calculation.
        Adjust the formula based on Aster's specific requirements.

        :param pair: Pair to calculate liquidation price for
        :param open_rate: Entry price of position
        :param is_short: True if the trade is a short, false otherwise
        :param amount: Absolute value of position size incl. leverage (in base currency)
        :param stake_amount: Stake amount - Collateral in settle currency.
        :param leverage: Leverage used for this position.
        :param wallet_balance: Amount of margin_mode in the wallet being used to trade
            Cross-Margin Mode: crossWalletBalance
            Isolated-Margin Mode: isolatedWalletBalance
        :param open_trades: List of open trades in the same wallet
        """
        cross_vars: float = 0.0

        # mm_ratio: Aster's formula specifies maintenance margin rate which is mm_ratio * 100%
        # maintenance_amt: (CUM) Maintenance Amount of position
        mm_ratio, maintenance_amt = self.get_maintenance_ratio_and_amt(pair, stake_amount)

        if self.margin_mode == MarginMode.CROSS:
            mm_ex_1: float = 0.0
            upnl_ex_1: float = 0.0
            pairs = [trade.pair for trade in open_trades]
            if self._config["runmode"] in ("live", "dry_run"):
                funding_rates = self.fetch_funding_rates(pairs)
            for trade in open_trades:
                if trade.pair == pair:
                    # Only "other" trades are considered
                    continue
                if self._config["runmode"] in ("live", "dry_run"):
                    mark_price = funding_rates[trade.pair]["markPrice"]
                else:
                    # Fall back to open rate for backtesting
                    mark_price = trade.open_rate
                mm_ratio1, maint_amnt1 = self.get_maintenance_ratio_and_amt(
                    trade.pair, trade.stake_amount
                )
                maint_margin = trade.amount * mark_price * mm_ratio1 - maint_amnt1
                mm_ex_1 += maint_margin

                upnl_ex_1 += trade.amount * mark_price - trade.amount * trade.open_rate

            cross_vars = upnl_ex_1 - mm_ex_1

        side_1 = -1 if is_short else 1

        if maintenance_amt is None:
            raise OperationalException(
                "Parameter maintenance_amt is required by Aster.liquidation_price "
                f"for {self.trading_mode}"
            )

        if self.trading_mode == TradingMode.FUTURES:
            return (
                (wallet_balance + cross_vars + maintenance_amt) - (side_1 * amount * open_rate)
            ) / ((amount * mm_ratio) - (side_1 * amount))
        else:
            raise OperationalException(
                "Freqtrade only supports isolated futures for leverage trading"
            )

    # def load_leverage_tiers(self) -> dict[str, list[dict]]:
    #     """
    #     Load leverage tiers for Aster.
    #     Uses live API calls like Bybit, with caching for performance.
    #     """
    #     if self.trading_mode == TradingMode.FUTURES:
    #         # Use the base Exchange class behavior - it will automatically
    #         # call get_leverage_tiers() which uses CCXT to fetch from Aster
    #         return super().load_leverage_tiers()
    #     else:
    #         return {}

    # Optional: Add caching like Bybit does
    # @retrier
    # def get_leverage_tiers(self) -> dict[str, list[dict]]:
    #     """
    #     Cache leverage tiers for 1 day, since they are not expected to change often.
    #     """
    #     # Load cached tiers
    #     tiers_cached = self.load_cached_leverage_tiers(
    #         self._config["stake_currency"], timedelta(days=1)
    #     )
    #     if tiers_cached:
    #         return tiers_cached
    #
    #     # Fetch tiers from exchange
    #     tiers = super().get_leverage_tiers()
    #
    #     self.cache_leverage_tiers(tiers, self._config["stake_currency"])
    #     return tiers

    async def _async_get_trade_history_id_startup(
        self, pair: str, since: int
    ) -> tuple[list[list], str]:
        """
        override for initial call

        Aster provides historic trades data through standard CCXT methods.
        Using from_id=0, we can get the earliest available trades.
        So if we don't get any data with the provided "since", we can assume to
        download all available data.
        """
        t, from_id = await self._async_fetch_trades(pair, since=since)
        if not t:
            return [], "0"
        return t, from_id

    async def _async_get_trade_history_id(
        self, pair: str, until: int, since: int, from_id: str | None = None
    ) -> tuple[str, list[list]]:
        logger.info(f"Fetching trades from Aster, {from_id=}, {since=}, {until=}")

        # Use standard CCXT methods for Aster
        return await super()._async_get_trade_history_id(
            pair, until=until, since=since, from_id=from_id
        )
