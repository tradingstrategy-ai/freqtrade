"""GRVT support."""
import logging
from typing import Any

from freqtrade.enums import TradingMode, MarginMode
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import FtHas

import ccxt

try:
    from pysdk.grvt_ccxt import GrvtCcxt
    from pysdk.grvt_ccxt_env import GrvtEnv
    from pysdk.grvt_ccxt_logging_selector import logger
    from pysdk.grvt_ccxt_test_utils import validate_return_values
    from pysdk.grvt_ccxt_utils import rand_uint32
except ImportError:
    GrvtCcxt = None


logger = logging.getLogger(__name__)


class Grvt(Exchange):
    """
    GRVT adapter.

    Using their own CCXT-like library.
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
        # "trades_has_history": False,  # Endpoint doesn't have a "since" parameter
        # "ws_enabled": True,
    }

    def _init_ccxt(
        self, exchange_config: dict[str, Any], sync: bool, ccxt_kwargs: dict[str, Any]
    ) -> ccxt.Exchange:
        """
        Initialize ccxt with given config and return valid ccxt instance.
        """
        # Find matching class for the given exchange name
        name = exchange_config["name"]
        assert sync, "async not supported by grvt-pysdk"
        ex_config = {
            "api_key": exchange_config.get("api_key"),
            "trading_account_id": exchange_config.get("trading_account_id"),
            "private_key": exchange_config.get("private_key"),
        }
        assert "api_key" in ex_config, "api_key is required"

        api = GrvtCcxt(
            GrvtEnv.PROD,
            logger,
            parameters=ex_config,
        )
        return api

    # TODO: This is a spoofed with Binance data.
    # Ask Orderly how to get.
    def fill_leverage_tiers(self) -> None:
        """
        Assigns property _leverage_tiers to a dictionary of information about the leverage
        allowed on each pair
        """

        # Spoofed data

        spoofed_data = {
          "tier": 1.0,
          "symbol": "1000000MOG/USDT:USDT",
          "currency": "USDT",
          "minNotional": 0.0,
          "maxNotional": 5000.0,
          "maintenanceMarginRate": 0.01,
          "maxLeverage": 25.0,
          "info": {
            "bracket": "1",
            "initialLeverage": "25",
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


_DUMMY_PAIRS = []


