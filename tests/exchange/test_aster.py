from unittest.mock import MagicMock, PropertyMock

import ccxt
import pytest

from freqtrade.enums import MarginMode
from freqtrade.exceptions import TemporaryError
from tests.conftest import get_patched_exchange


def _make_aster(mocker, conf):
    """Return a patched Aster exchange instance with setMarginMode capability."""
    api_mock = MagicMock()
    type(api_mock).has = PropertyMock(return_value={"setMarginMode": True})
    conf["dry_run"] = False
    return get_patched_exchange(mocker, conf, api_mock, exchange="aster"), api_mock


def test_set_margin_mode_nochange_is_ignored(mocker, default_conf):
    """Aster -4046 NoChange must be treated as success, not a TemporaryError."""
    exchange, api_mock = _make_aster(mocker, default_conf)

    api_mock.set_margin_mode = MagicMock(side_effect=ccxt.NoChange("No need to change margin type"))

    # Should not raise — NoChange means the mode is already correctly set.
    exchange.set_margin_mode("BTC/USDT:USDT", MarginMode.ISOLATED)


def test_set_margin_mode_nochange_accept_fail_also_ignored(mocker, default_conf):
    """NoChange is ignored regardless of accept_fail value."""
    exchange, api_mock = _make_aster(mocker, default_conf)

    api_mock.set_margin_mode = MagicMock(side_effect=ccxt.NoChange("No need to change margin type"))

    exchange.set_margin_mode("BTC/USDT:USDT", MarginMode.ISOLATED, accept_fail=True)
    exchange.set_margin_mode("BTC/USDT:USDT", MarginMode.ISOLATED, accept_fail=False)


def test_set_margin_mode_other_errors_still_raise(mocker, default_conf):
    """Non-NoChange TemporaryErrors must still propagate."""
    exchange, api_mock = _make_aster(mocker, default_conf)

    api_mock.set_margin_mode = MagicMock(side_effect=ccxt.OperationRejected("Some other rejection"))

    with pytest.raises(TemporaryError):
        exchange.set_margin_mode("BTC/USDT:USDT", MarginMode.ISOLATED)


def test_set_margin_mode_success_unchanged(mocker, default_conf):
    """Normal successful set_margin_mode call still works."""
    exchange, api_mock = _make_aster(mocker, default_conf)

    api_mock.set_margin_mode = MagicMock(return_value={"code": 200})

    exchange.set_margin_mode("BTC/USDT:USDT", MarginMode.ISOLATED)
    assert api_mock.set_margin_mode.call_count == 1
