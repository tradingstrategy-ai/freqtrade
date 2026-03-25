"""Mode exchange subclass"""

import logging
from typing import Any

import ccxt

from freqtrade.enums.marginmode import MarginMode
from freqtrade.enums.tradingmode import TradingMode
from freqtrade.exceptions import (
    DDosProtection,
    OperationalException,
    TemporaryError,
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange_types import FtHas, OrderBook


logger = logging.getLogger(__name__)


class Modetrade(Exchange):
    """
    Mode exchange class. Contains adjustments needed for Freqtrade to work
    with this exchange.

    Mode is a "broker" (frontend) for Orderly backend.

    `Find more information on Orderly here <https://orderly.network/docs/home>`__.
    """

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        # TradingMode.SPOT always supported and not required in this list
        # (TradingMode.MARGIN, MarginMode.CROSS),
        # (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    _ft_has: FtHas = {
        # TODO: Confirm correct parameters from Orderly
        "mark_ohlcv_timeframe": "1d",
        # TODO: Confirm correct parameters from Orderly
        "funding_fee_timeframe": "1d",
        # "stoploss_order_types": {"limit": "limit"},
        # "stoploss_on_exchange": True,
        # "trades_has_history": False,  # Endpoint doesn"t have a "since" parameter
        # "ws_enabled": True,
    }

    def __init__(self, config: dict[str, Any], *, validate: bool = True, **kwargs) -> None:
        """Initialize ModeTrade exchange with delisting detection."""
        super().__init__(config, validate=validate, **kwargs)
        # Track BadSymbol failures per pair for delisting detection
        self._bad_symbol_count: dict[str, int] = {}
        self._delisted_pairs: set[str] = set()
        # Mark pair as delisted after N consecutive BadSymbol failures
        self._bad_symbol_threshold = 3
        logger.info(
            f"ModeTrade delisting detection enabled "
            f"(threshold: {self._bad_symbol_threshold} failures)"
        )

    def _modetrade_price_sanity_cfg(self) -> dict[str, Any]:
        cfg = {}
        if isinstance(self._config, dict):
            # New generic key (preferred)
            cfg = self._config.get("price_sanity_check_settings", {}) or {}
            # Backward-compatible key (older configs)
            if not cfg:
                cfg = self._config.get("modetrade_price_sanity", {}) or {}
        if not isinstance(cfg, dict):
            cfg = {}
        return {
            "enabled": bool(cfg.get("enabled", True)),
            "max_deviation_ratio": float(cfg.get("max_deviation_ratio", 0.03)),
            "log_level": str(cfg.get("log_level", "warning")).lower(),
        }

    @staticmethod
    def _rel_deviation(a: float, b: float) -> float:
        """Relative deviation |a-b|/|b| (b as reference)."""
        if b == 0:
            return float("inf")
        return abs(a - b) / abs(b)

    def _fetch_last_trade_price(self, pair: str) -> tuple[float | None, str | None]:
        try:
            trades = self._api.fetch_trades(pair, limit=10)
            if not trades:
                return None, None
            # CCXT does not guarantee ordering, so pick the newest by timestamp.
            newest = max(
                (t for t in trades if isinstance(t, dict) and t.get("price") is not None),
                key=lambda t: t.get("timestamp") or 0,
                default=None,
            )
            if not newest:
                return None, None
            return float(newest["price"]), None
        except Exception as e:
            return None, str(e)

    def _fetch_last_ohlcv_close(self, pair: str) -> tuple[float | None, str | None]:
        try:
            ohlcv = self._api.fetch_ohlcv(pair, timeframe="1m", limit=2)
            if not ohlcv:
                return None, None
            last = ohlcv[-1]
            # [timestamp, open, high, low, close, volume]
            if isinstance(last, (list, tuple)) and len(last) >= 5 and last[4] is not None:
                return float(last[4]), None
            return None, None
        except Exception as e:
            return None, str(e)

    @staticmethod
    def _ticker_ref(ticker: Any | None) -> tuple[float | None, float | None]:
        """
        Extract (index_price, mark_price) from a CCXT ticker.
        We store these under ticker["info"]["index_price"/"mark_price"] in our CCXT adapter.
        """
        if not isinstance(ticker, dict):
            return None, None
        info = ticker.get("info")
        if not isinstance(info, dict):
            return None, None
        idx = info.get("index_price")
        mark = info.get("mark_price")
        try:
            idx = float(idx) if idx is not None else None
        except Exception:
            idx = None
        try:
            mark = float(mark) if mark is not None else None
        except Exception:
            mark = None
        return idx, mark

    @staticmethod
    def _ob_top(order_book: Any | None) -> tuple[float | None, float | None]:
        """
        Extract top-of-book bid/ask (best bid, best ask) from a ccxt-style orderbook dict.
        """
        if not isinstance(order_book, dict):
            return None, None
        bid = ask = None
        bids = order_book.get("bids")
        asks = order_book.get("asks")
        if isinstance(bids, list) and bids:
            try:
                bid = float(bids[0][0])
            except Exception:
                bid = None
        if isinstance(asks, list) and asks:
            try:
                ask = float(asks[0][0])
            except Exception:
                ask = None
        return bid, ask

    def get_rate(
        self,
        pair: str,
        refresh: bool,
        side: str,
        is_short: bool,
        order_book: Any | None = None,
        ticker: Any | None = None,
    ) -> float:
        """
        Get rate with sanity check for thin liquidity markets.

        For market orders, compares order book price against mark/index.
        Uses order book when reasonable, falls back to mark/index for outliers.
        This prevents false stoploss triggers from spiky order book data.

        Also handles delisted pairs by falling back to ticker pricing.
        """
        # If pair is marked as delisted, skip order book and use ticker only
        if pair in self._delisted_pairs:
            logger.warning(
                f"Pair {pair} is delisted. Using ticker pricing instead of order book."
            )
            if ticker is None:
                ticker = self.fetch_ticker(pair)
            # Use ticker-only pricing by disabling order book
            return super().get_rate(pair, refresh, side, is_short, order_book=None, ticker=ticker)

        cfg = self._modetrade_price_sanity_cfg()
        if not cfg["enabled"]:
            return super().get_rate(pair, refresh, side, is_short, order_book=order_book, ticker=ticker)

        # Get fresh order book price
        # This may raise DDosProtection if pair is delisted (caught by fetch_l2_order_book)
        try:
            ob_rate = super().get_rate(pair, True, side, is_short, order_book=None, ticker=None)
        except DDosProtection as e:
            if "delisted" in str(e).lower():
                # Pair just got marked as delisted - use ticker fallback
                logger.warning(
                    f"Pair {pair} marked as delisted during get_rate. "
                    f"Falling back to ticker pricing."
                )
                if ticker is None:
                    ticker = self.fetch_ticker(pair)
                # Use ticker-only pricing
                return super().get_rate(pair, refresh, side, is_short, order_book=None, ticker=ticker)
            raise

        # Get mark/index reference
        idx, mark, ref_err = self._get_reference_price(pair, ticker)
        ref_price = idx if idx is not None else mark

        # Determine which price to use
        if ref_price is not None:
            deviation = self._rel_deviation(ob_rate, ref_price)
            max_dev = cfg["max_deviation_ratio"]

            if deviation <= max_dev:
                chosen_rate = ob_rate
                action = "use_orderbook"
            else:
                chosen_rate = ref_price
                action = f"use_{'index' if idx is not None else 'mark'}"
        else:
            chosen_rate = ob_rate
            action = "use_orderbook_no_ref"
            deviation = None

        # Log decision
        self._log_price_decision({
            "pair": pair,
            "side": side,
            "is_short": is_short,
            "ob_rate": ob_rate,
            "idx": idx,
            "mark": mark,
            "chosen_rate": chosen_rate,
            "action": action,
            "deviation": deviation,
            "max_dev": cfg["max_deviation_ratio"],
            "ref_err": ref_err,
            "log_level": cfg["log_level"],
        })

        return chosen_rate

    def _get_reference_price(
        self, pair: str, ticker: Any | None
    ) -> tuple[float | None, float | None, str | None]:
        """Fetch index and mark prices, return (index, mark, error)."""
        try:
            t = ticker if isinstance(ticker, dict) else self._api.fetch_ticker(pair)
            idx, mark = self._ticker_ref(t)
            return idx, mark, None
        except Exception as e:
            return None, None, str(e)

    def _log_price_decision(self, data: dict[str, Any]) -> None:
        """Log price sanity check decision."""
        dev_str = f"{data['deviation']:.2%}" if data['deviation'] is not None else "N/A"
        idx_str = f"{data['idx']:.6f}" if data['idx'] is not None else "N/A"
        mark_str = f"{data['mark']:.6f}" if data['mark'] is not None else "N/A"

        msg = (
            f"ModeTrade price check: {data['action']} | "
            f"pair={data['pair']} side={data['side']} short={data['is_short']} | "
            f"ob={data['ob_rate']:.6f} idx={idx_str} "
            f"mark={mark_str} → chose={data['chosen_rate']:.6f} | "
            f"dev={dev_str} max={data['max_dev']:.1%}"
        )

        if data['ref_err']:
            msg += f" | ref_err={data['ref_err']}"

        getattr(logger, data['log_level'], logger.warning)(msg)

    # TODO: This is a spoofed with Binance data.
    # Ask Orderly how to get.
    def fill_leverage_tiers(self) -> None:
        """
        Assigns property _leverage_tiers to a dictionary of information about the leverage
        allowed on each pair
        """

        # Spoofed data
        spoofed_data = {
            "tier": 1,
            "symbol": "1000000MOG/USDT:USDT",
            "currency": "USDT",
            "minNotional": 0.0,
            "maxNotional": 5000.0,
            "maintenanceMarginRate": 0.01,
            "maxLeverage": 10,
            "info": {
                "bracket": "1",
                "initialLeverage": "1",
                "notionalCap": "5000",
                "notionalFloor": "0",
                "maintMarginRatio": "0.01",
                "cum": "0.0"
            }
        }

        # leverage_tiers = self.load_leverage_tiers()
        # for pair, tiers in leverage_tiers.items():
        #     pair_tiers = []
        #     for tier in tiers:
        #         pair_tiers.append(self.parse_leverage_tier(tier))
        #     self._leverage_tiers[pair] = pair_tiers

        # Generate 1 fake leverage tier per pair
        for pair in _DUMMY_PAIRS:
            data = spoofed_data.copy()
            data["symbol"] = pair
            self._leverage_tiers[pair] = [
                self.parse_leverage_tier(data)
            ]

    def validate_ordertypes(self, order_types: dict) -> None:
        """
        Override the default validate_ordertypes to remove the market order check
        since ccxt config is wrong
        """
        # if any(v == "market" for k, v in order_types.items()):
        #     if not self.exchange_has("createMarketOrder"):
        #         raise ConfigurationError(f"Exchange {self.name} does not support market orders.")
        self.validate_stop_ordertypes(order_types)

    @retrier
    def additional_exchange_init(self) -> None:
        """
        Additional exchange initialization logic.
        Checks for positions on exchange that don't match the configured whitelist.
        """
        try:
            if self._config["dry_run"] or self.trading_mode != TradingMode.FUTURES:
                return

            positions = self.fetch_positions()
            open_positions = [p for p in positions if p.get('contracts', 0) != 0]

            if not open_positions:
                logger.info("ModeTrade: No open positions on exchange")
                return

            whitelist = self._config.get('exchange', {}).get('pair_whitelist', [])
            unauthorized = [p for p in open_positions if p['symbol'] not in whitelist]

            logger.info(f"ModeTrade: Found {len(open_positions)} open position(s) on exchange")

            if unauthorized:
                logger.warning(
                    f"ModeTrade: {len(unauthorized)} position(s) not in whitelist "
                    f"(may be from another bot or manual trades):"
                )
                for pos in unauthorized:
                    logger.warning(f"  {pos['symbol']}: {pos.get('contracts')} contracts")

        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def fetch_l2_order_book(self, pair: str, limit: int = 100) -> OrderBook:
        """
        Override to track BadSymbol errors and detect delisted pairs.

        After N consecutive BadSymbol failures, marks the pair as delisted
        and raises DDosProtection to trigger emergency handling.
        """
        # If already marked as delisted, raise immediately with clear message
        if pair in self._delisted_pairs:
            logger.warning(
                f"Skipping {pair} - already marked as delisted. "
                f"Add to pair_blacklist in config to silence this warning."
            )
            raise DDosProtection(
                f"Pair {pair} has been delisted from exchange. "
                f"Add to blacklist to remove from trading."
            )

        try:
            order_book = super().fetch_l2_order_book(pair, limit)
            # Success - reset failure count for this pair
            self._bad_symbol_count.pop(pair, None)
            return order_book

        except TemporaryError as e:
            # Check if this was caused by BadSymbol
            is_bad_symbol = (
                isinstance(e.__cause__, ccxt.BadSymbol) or
                "BadSymbol" in str(e) or
                "does not have market symbol" in str(e)
            )

            if is_bad_symbol:
                # Track consecutive BadSymbol failures
                self._bad_symbol_count[pair] = self._bad_symbol_count.get(pair, 0) + 1
                failure_count = self._bad_symbol_count[pair]

                if failure_count >= self._bad_symbol_threshold:
                    # Mark as delisted
                    self._delisted_pairs.add(pair)

                    # Add to runtime blacklist to prevent new entries
                    # This does NOT modify the config file - only in-memory blacklist
                    added_to_blacklist = False
                    if pair not in self._config.get("exchange", {}).get("pair_blacklist", []):
                        if "exchange" not in self._config:
                            self._config["exchange"] = {}
                        if "pair_blacklist" not in self._config["exchange"]:
                            self._config["exchange"]["pair_blacklist"] = []

                        self._config["exchange"]["pair_blacklist"].append(pair)
                        added_to_blacklist = True
                        logger.info(
                            f"✓ Added {pair} to runtime pair_blacklist (in-memory only, "
                            f"not saved to config file)"
                        )

                    # Log clear actionable message
                    if added_to_blacklist:
                        logger.error(
                            f"⚠️  PAIR DELISTED: {pair} failed with BadSymbol {failure_count} times. "
                            f"This pair appears to be delisted from the exchange. "
                            f"Automatically added to runtime blacklist. "
                            f"RECOMMENDED: Add '{pair}' to pair_blacklist in your config file for persistence."
                        )
                    else:
                        logger.error(
                            f"⚠️  PAIR DELISTED: {pair} failed with BadSymbol {failure_count} times. "
                            f"This pair appears to be delisted from the exchange. "
                            f"Pair already in blacklist - no new entry attempts will be made."
                        )

                    # Raise DDosProtection to prevent infinite retry loop
                    # This stops the bot from continuing to hammer this pair
                    raise DDosProtection(
                        f"Pair {pair} has been delisted from exchange "
                        f"(BadSymbol threshold {self._bad_symbol_threshold} reached)"
                    ) from e
                else:
                    # Still tracking, log progress
                    logger.warning(
                        f"BadSymbol error for {pair} ({failure_count}/{self._bad_symbol_threshold}). "
                        f"Will mark as delisted if this continues."
                    )
                    # Re-raise for retry
                    raise
            else:
                # Not a BadSymbol error, just re-raise
                raise

    @retrier
    def _set_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        """
        Override the default set_leverage to use integer leverage
        """
        if self._config["dry_run"] or not self.exchange_has("setLeverage"):
            # Some exchanges only support one margin_mode type
            return

        # Orderly requires integer leverage
        leverage = int(leverage)

        try:
            res = self._api.set_leverage(symbol=pair, leverage=leverage)
            self._log_exchange_response("set_leverage", res)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected, ccxt.InsufficientFunds) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

_DUMMY_PAIRS = [
    "1000000MOG/USDC:USDC",
    "1000APU/USDC:USDC",
    "1000BONK/USDC:USDC",
    "1000CAT/USDC:USDC",
    "1000FLOKI/USDC:USDC",
    "1000PEPE/USDC:USDC",
    "1000SHIB/USDC:USDC",
    "AAVE/USDC:USDC",
    "ACT/USDC:USDC",
    "ADA/USDC:USDC",
    "AI16Z/USDC:USDC",
    "AIXBT/USDC:USDC",
    "ANIME/USDC:USDC",
    "APT/USDC:USDC",
    "ARB/USDC:USDC",
    "AR/USDC:USDC",
    "ASTER/USDC:USDC",
    "AVAX/USDC:USDC",
    "AVNT/USDC:USDC",
    "BERA/USDC:USDC",
    "BIO/USDC:USDC",
    "BMT/USDC:USDC",
    "BNB/USDC:USDC",
    "BCH/USDC:USDC",
    "BOME/USDC:USDC",
    "BRETT/USDC:USDC",
    "BTC/USDC:USDC",
    "C98/USDC:USDC",
    "CAKE/USDC:USDC",
    "CETUS/USDC:USDC",
    "CHILLGUY/USDC:USDC",
    "COOKIE/USDC:USDC",
    "CRO/USDC:USDC",
    "CRV/USDC:USDC",
    "DOGE/USDC:USDC",
    "DOOD/USDC:USDC",
    "DOT/USDC:USDC",
    "EIGEN/USDC:USDC",
    "ENA/USDC:USDC",
    "ETHFI/USDC:USDC",
    "ETH/USDC:USDC",
    "FARTCOIN/USDC:USDC",
    "FET/USDC:USDC",
    "FUN/USDC:USDC",
    "GOAT/USDC:USDC",
    "GRASS/USDC:USDC",
    "HBAR/USDC:USDC",
    "HOME/USDC:USDC",
    "HSK/USDC:USDC",
    "HYPER/USDC:USDC",
    "HYPE/USDC:USDC",
    "H/USDC:USDC",
    "ICNT/USDC:USDC",
    "INJ/USDC:USDC",
    "IO/USDC:USDC",
    "IP/USDC:USDC",
    "JUP/USDC:USDC",
    "KAITO/USDC:USDC",
    "KNC/USDC:USDC",
    "LAUNCHCOIN/USDC:USDC",
    "LDO/USDC:USDC",
    "LINEA/USDC:USDC",
    "LINK/USDC:USDC",
    "LOKA/USDC:USDC",
    "LPT/USDC:USDC",
    "LTC/USDC:USDC",
    "MAGIC/USDC:USDC",
    "MELANIA/USDC:USDC",
    "MERL/USDC:USDC",
    "MET/USDC:USDC",
    "MEW/USDC:USDC",
    "MKR/USDC:USDC",
    "MNT/USDC:USDC",
    "MODE/USDC:USDC",
    "MOODENG/USDC:USDC",
    "MOVE/USDC:USDC",
    "MUBARAK/USDC:USDC",
    "NEAR/USDC:USDC",
    "NEIRO/USDC:USDC",
    "NEWT/USDC:USDC",
    "NXPC/USDC:USDC",
    "ONDO/USDC:USDC",
    "OP/USDC:USDC",
    "ORDER/USDC:USDC",
    "ORDI/USDC:USDC",
    "PAXG/USDC:USDC",
    "PENDLE/USDC:USDC",
    "PENGU/USDC:USDC",
    "PLUME/USDC:USDC",
    "PNUT/USDC:USDC",
    "POL/USDC:USDC",
    "PONKE/USDC:USDC",
    "POPCAT/USDC:USDC",
    "PUMP/USDC:USDC",
    "PYTH/USDC:USDC",
    "RAY/USDC:USDC",
    "SAHARA/USDC:USDC",
    "SEI/USDC:USDC",
    "SKY/USDC:USDC",
    "SOL/USDC:USDC",
    "SOPH/USDC:USDC",
    "SPX/USDC:USDC",
    "STBL/USDC:USDC",
    "STO/USDC:USDC",
    "STRK/USDC:USDC",
    "SUI/USDC:USDC",
    "SYRUP/USDC:USDC",
    "S/USDC:USDC",
    "TAO/USDC:USDC",
    "TIA/USDC:USDC",
    "TON/USDC:USDC",
    "TRUMP/USDC:USDC",
    "TRX/USDC:USDC",
    "TST/USDC:USDC",
    "TURBO/USDC:USDC",
    "UNI/USDC:USDC",
    "USELESS/USDC:USDC",
    "VIC/USDC:USDC",
    "VINE/USDC:USDC",
    "VIRTUAL/USDC:USDC",
    "WAL/USDC:USDC",
    "WCT/USDC:USDC",
    "WIF/USDC:USDC",
    "WLD/USDC:USDC",
    "WOO/USDC:USDC",
    "WLFI/USDC:USDC",
    "W/USDC:USDC",
    "XLM/USDC:USDC",
    "XPL/USDC:USDC",
    "XRP/USDC:USDC",
    "ZEN/USDC:USDC",
    "ZEUS/USDC:USDC",
    "ZEC/USDC:USDC",
    "ZORA/USDC:USDC",
    "ZRO/USDC:USDC",
    "0G/USDC:USDC"
]
