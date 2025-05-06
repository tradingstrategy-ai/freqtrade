"""WOOFi Pro exchange subclass"""

import logging

from freqtrade.exchange import Exchange

from deps.freqtrade.freqtrade.enums.marginmode import MarginMode
from deps.freqtrade.freqtrade.enums.tradingmode import TradingMode

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
