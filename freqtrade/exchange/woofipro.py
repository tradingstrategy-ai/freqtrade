"""WOOFi Pro exchange subclass"""

import logging


from freqtrade.exchange import Exchange

from freqtrade.enums.marginmode import MarginMode
from freqtrade.enums.tradingmode import TradingMode
from freqtrade.exchange.exchange_types import FtHas


logger = logging.getLogger(__name__)


class Woofipro(Exchange):
    """
    WOOFi Pro exchange class. Contains adjustments needed for Freqtrade to work
    with this exchange.

    WOOFi Pro is a "broker" (frontend) for Orderly backend.

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
        # "trades_has_history": False,  # Endpoint doesn't have a "since" parameter
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


_DUMMY_PAIRS = [
      "1000000MOG/USDC:USDC",
      "1000APU/USDC:USDC",
      "1000BONK/USDC:USDC",
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
      "BABY/USDC:USDC",
      "BANANAS31/USDC:USDC",
      "BERA/USDC:USDC",
      "BIGTIME/USDC:USDC",
      "BIO/USDC:USDC",
      "BLUR/USDC:USDC",
      "BMT/USDC:USDC",
      "BNB/USDC:USDC",
      "BOME/USDC:USDC",
      "BRETT/USDC:USDC",
      "BTC/USDC:USDC",
      "C98/USDC:USDC",
      "CAKE/USDC:USDC",
      "CETUS/USDC:USDC",
      "COW/USDC:USDC",
      "CRV/USDC:USDC",
      "DOGE/USDC:USDC",
      "EIGEN/USDC:USDC",
      "ELX/USDC:USDC",
      "ENA/USDC:USDC",
      "ETHFI/USDC:USDC",
      "ETH/USDC:USDC",
      "FARTCOIN/USDC:USDC",
      "FIL/USDC:USDC",
      "GOAT/USDC:USDC",
      "GPS/USDC:USDC",
      "GRIFFAIN/USDC:USDC",
      "HBAR/USDC:USDC",
      "HSK/USDC:USDC",
      "HYPER/USDC:USDC",
      "HYPE/USDC:USDC",
      "INIT/USDC:USDC",
      "INJ/USDC:USDC",
      "IO/USDC:USDC",
      "IP/USDC:USDC",
      "JUP/USDC:USDC",
      "KAITO/USDC:USDC",
      "LAYER/USDC:USDC",
      "LDO/USDC:USDC",
      "LINK/USDC:USDC",
      "LTC/USDC:USDC",
      "MELANIA/USDC:USDC",
      "MERL/USDC:USDC",
      "MEW/USDC:USDC",
      "MKR/USDC:USDC",
      "MODE/USDC:USDC",
      "MOODENG/USDC:USDC",
      "MOVE/USDC:USDC",
      "MUBARAK/USDC:USDC",
      "NEAR/USDC:USDC",
      "NEIRO/USDC:USDC",
      "NIL/USDC:USDC",
      "OMNI/USDC:USDC",
      "OM/USDC:USDC",
      "ONDO/USDC:USDC",
      "OP/USDC:USDC",
      "ORDER/USDC:USDC",
      "ORDI/USDC:USDC",
      "PARTI/USDC:USDC",
      "PAXG/USDC:USDC",
      "PENDLE/USDC:USDC",
      "PENGU/USDC:USDC",
      "PI/USDC:USDC",
      "PLUME/USDC:USDC",
      "PNUT/USDC:USDC",
      "POL/USDC:USDC",
      "PONKE/USDC:USDC",
      "POPCAT/USDC:USDC",
      "PROMPT/USDC:USDC",
      "RAY/USDC:USDC",
      "RED/USDC:USDC",
      "RFC/USDC:USDC",
      "RUNE/USDC:USDC",
      "SEI/USDC:USDC",
      "SHELL/USDC:USDC",
      "SOL/USDC:USDC",
      "SPX/USDC:USDC",
      "STRK/USDC:USDC",
      "SUI/USDC:USDC",
      "S/USDC:USDC",
      "TAO/USDC:USDC",
      "TIA/USDC:USDC",
      "TON/USDC:USDC",
      "TRUMP/USDC:USDC",
      "TRX/USDC:USDC",
      "TST/USDC:USDC",
      "TURBO/USDC:USDC",
      "VIC/USDC:USDC",
      "VINE/USDC:USDC",
      "VIRTUAL/USDC:USDC",
      "WAL/USDC:USDC",
      "WCT/USDC:USDC",
      "WIF/USDC:USDC",
      "WLD/USDC:USDC",
      "WOO/USDC:USDC",
      "W/USDC:USDC",
      "XAUT/USDC:USDC",
      "XRP/USDC:USDC",
      "ZEN/USDC:USDC",
      "ZEUS/USDC:USDC",
      "ZK/USDC:USDC",
      "ZORA/USDC:USDC",
      "ZRO/USDC:USDC"
    ]