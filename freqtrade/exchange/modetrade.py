"""Mode exchange subclass"""

import logging
from copy import deepcopy
from decimal import Decimal

import ccxt
from freqtrade.enums.marginmode import MarginMode
from freqtrade.enums.tradingmode import TradingMode
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import CcxtBalances, FtHas
from freqtrade.exchange.common import retrier
from freqtrade.exceptions import (
    DDosProtection,
    OperationalException,
    TemporaryError,
)


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

    # @retrier
    # def get_balances(self) -> CcxtBalances:
    #     """
    #     Override the default get_balances to add values for "free" and "used"
    #     """
    #     balances = super().get_balances()
    #     new_balances = deepcopy(balances)
    #     for token, balance in balances.items():
    #         if balance["free"] is None:
    #             new_balances[token]["frozen"] = float(balance["frozen"])
    #             new_balances[token]["free"] = float(Decimal(balance["total"]) - Decimal(balance["frozen"]))
    #             new_balances[token]["used"] = float(balance["frozen"])
    #     return new_balances

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
    "AVAX/USDC:USDC",
    "BERA/USDC:USDC",
    "BIO/USDC:USDC",
    "BMT/USDC:USDC",
    "BNB/USDC:USDC",
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
    "LINK/USDC:USDC",
    "LOKA/USDC:USDC",
    "LPT/USDC:USDC",
    "LTC/USDC:USDC",
    "MAGIC/USDC:USDC",
    "MELANIA/USDC:USDC",
    "MERL/USDC:USDC",
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
    "RAY/USDC:USDC",
    "SAHARA/USDC:USDC",
    "SEI/USDC:USDC",
    "SOL/USDC:USDC",
    "SOPH/USDC:USDC",
    "SPX/USDC:USDC",
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
    "USELESS/USDC:USDC",
    "VIC/USDC:USDC",
    "VINE/USDC:USDC",
    "VIRTUAL/USDC:USDC",
    "WAL/USDC:USDC",
    "WCT/USDC:USDC",
    "WIF/USDC:USDC",
    "WLD/USDC:USDC",
    "WOO/USDC:USDC",
    "W/USDC:USDC",
    "XLM/USDC:USDC",
    "XRP/USDC:USDC",
    "ZEN/USDC:USDC",
    "ZEUS/USDC:USDC",
    "ZRO/USDC:USDC"
]