import asyncio
import logging
import threading
from datetime import timedelta
from types import SimpleNamespace
from time import sleep
from unittest.mock import AsyncMock, MagicMock

import pytest
from ccxt import NotSupported

from freqtrade.enums import CandleType
from freqtrade.exchange.exchange import Exchange
from freqtrade.exchange.exchange_ws import ExchangeWS
from ft_client.test_client.test_rest_client import log_has_re


def test_exchangews_init(mocker):
    config = MagicMock()
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    sleep(0.1)

    assert exchange_ws.config == config
    assert exchange_ws._ccxt_object == ccxt_object
    assert exchange_ws._thread.name == "ccxt_ws"
    assert exchange_ws._background_tasks == set()
    assert exchange_ws._klines_watching == set()
    assert exchange_ws._klines_scheduled == set()
    assert exchange_ws.klines_last_refresh == {}
    assert exchange_ws.klines_last_request == {}
    # Cleanup
    exchange_ws.cleanup()


def test_exchangews_cleanup_error(mocker, caplog):
    config = MagicMock()
    ccxt_object = MagicMock()
    ccxt_object.close = AsyncMock(side_effect=Exception("Test"))
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    patch_eventloop_threading(exchange_ws)

    sleep(0.1)
    exchange_ws.reset_connections()

    assert log_has_re("Exception in _cleanup_async", caplog)

    exchange_ws.cleanup()


def patch_eventloop_threading(exchange):
    init_event = threading.Event()

    def thread_func():
        exchange._loop = asyncio.new_event_loop()
        init_event.set()
        exchange._loop.run_forever()

    x = threading.Thread(target=thread_func, daemon=True)
    x.start()
    # Wait for thread to be properly initialized with timeout
    if not init_event.wait(timeout=5.0):
        raise RuntimeError("Failed to initialize event loop thread")


async def test_exchangews_ohlcv(mocker, time_machine, caplog):
    config = MagicMock()
    ccxt_object = MagicMock()
    caplog.set_level(logging.DEBUG)

    async def controlled_sleeper(*args, **kwargs):
        # Sleep to pass control back to the event loop
        await asyncio.sleep(0.1)
        return MagicMock()

    async def wait_for_condition(condition_func, timeout_=5.0, check_interval=0.01):
        """Wait for a condition to be true with timeout."""
        try:
            async with asyncio.timeout(timeout_):
                while True:
                    if condition_func():
                        return True
                    await asyncio.sleep(check_interval)
        except TimeoutError:
            return False

    ccxt_object.un_watch_ohlcv_for_symbols = AsyncMock(side_effect=[NotSupported, ValueError])
    ccxt_object.watch_ohlcv = AsyncMock(side_effect=controlled_sleeper)
    ccxt_object.close = AsyncMock()
    time_machine.move_to("2024-11-01 01:00:02 +00:00")

    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    patch_eventloop_threading(exchange_ws)
    try:
        assert exchange_ws._klines_watching == set()
        assert exchange_ws._klines_scheduled == set()

        exchange_ws.schedule_ohlcv("ETH/BTC", "1m", CandleType.SPOT)
        exchange_ws.schedule_ohlcv("XRP/BTC", "1m", CandleType.SPOT)

        # Wait for both pairs to be properly scheduled and watching
        await wait_for_condition(
            lambda: (
                len(exchange_ws._klines_watching) == 2 and len(exchange_ws._klines_scheduled) == 2
            ),
            timeout_=2.0,
        )

        assert exchange_ws._klines_watching == {
            ("ETH/BTC", "1m", CandleType.SPOT),
            ("XRP/BTC", "1m", CandleType.SPOT),
        }
        assert exchange_ws._klines_scheduled == {
            ("ETH/BTC", "1m", CandleType.SPOT),
            ("XRP/BTC", "1m", CandleType.SPOT),
        }

        # Wait for the expected number of watch calls
        await wait_for_condition(lambda: ccxt_object.watch_ohlcv.call_count >= 6, timeout_=3.0)
        assert ccxt_object.watch_ohlcv.call_count >= 6
        ccxt_object.watch_ohlcv.reset_mock()

        time_machine.shift(timedelta(minutes=5))
        exchange_ws.schedule_ohlcv("ETH/BTC", "1m", CandleType.SPOT)

        # Wait for log message
        await wait_for_condition(
            lambda: log_has_re("un_watch_ohlcv_for_symbols not supported: ", caplog), timeout_=2.0
        )
        assert log_has_re("un_watch_ohlcv_for_symbols not supported: ", caplog)

        # XRP/BTC should be cleaned up.
        assert exchange_ws._klines_watching == {
            ("ETH/BTC", "1m", CandleType.SPOT),
        }

        # Cleanup happened.
        exchange_ws.schedule_ohlcv("ETH/BTC", "1m", CandleType.SPOT)

        # Verify final state
        assert exchange_ws._klines_watching == {
            ("ETH/BTC", "1m", CandleType.SPOT),
        }
        assert exchange_ws._klines_scheduled == {
            ("ETH/BTC", "1m", CandleType.SPOT),
        }

        # Triggers 2nd call to un_watch_ohlcv_for_symbols which raises ValueError
        exchange_ws._klines_watching.discard(("ETH/BTC", "1m", CandleType.SPOT))
        await wait_for_condition(
            lambda: log_has_re("Exception in _unwatch_ohlcv", caplog), timeout_=2.0
        )
        assert log_has_re("Exception in _unwatch_ohlcv", caplog)

    finally:
        # Cleanup
        exchange_ws.cleanup()


async def test_ip_pool_fallback_failure_is_ignored(mocker, caplog):
    config = {"exchange": {"websocket_ip_pool": ["127.0.0.2"]}}
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())
    mocker.patch(
        "freqtrade.exchange.exchange_ws.ExchangeWS._create_ws_exchange_pool",
        return_value={"127.0.0.2": ccxt_object},
    )
    caplog.set_level(logging.WARNING)

    exchange_ws = ExchangeWS(config, ccxt_object)
    try:
        await exchange_ws._handle_ip_failure("default", "BTC/USDT:USDT")
    finally:
        exchange_ws.cleanup()

    assert exchange_ws._ip_consecutive_failures == {}
    assert log_has_re("Ignoring failure for untracked fallback IP default", caplog)


def test_ws_scheduled_refresh_default_depends_on_pool_size(mocker):
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())
    mocker.patch(
        "freqtrade.exchange.exchange_ws.ExchangeWS._create_ws_exchange_pool",
        return_value={"127.0.0.2": ccxt_object, "127.0.0.3": ccxt_object},
    )

    no_pool = ExchangeWS({"exchange": {}}, ccxt_object)
    one_ip = ExchangeWS({"exchange": {"websocket_ip_pool": ["127.0.0.2"]}}, ccxt_object)
    two_ips = ExchangeWS(
        {"exchange": {"websocket_ip_pool": ["127.0.0.2", "127.0.0.3"]}},
        ccxt_object,
    )
    try:
        assert no_pool.ws_scheduled_refresh_enabled is False
        assert one_ip.ws_scheduled_refresh_enabled is False
        assert two_ips.ws_scheduled_refresh_enabled is True
    finally:
        no_pool.cleanup()
        one_ip.cleanup()
        two_ips.cleanup()


def test_ws_scheduled_refresh_explicit_false_overrides_multi_ip_pool(mocker):
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())
    mocker.patch(
        "freqtrade.exchange.exchange_ws.ExchangeWS._create_ws_exchange_pool",
        return_value={"127.0.0.2": ccxt_object, "127.0.0.3": ccxt_object},
    )

    exchange_ws = ExchangeWS(
        {
            "exchange": {
                "websocket_ip_pool": ["127.0.0.2", "127.0.0.3"],
                "ws_scheduled_refresh_enabled": False,
            }
        },
        ccxt_object,
    )
    try:
        assert exchange_ws.ws_scheduled_refresh_enabled is False
    finally:
        exchange_ws.cleanup()


def test_ws_connection_reset_respects_scheduled_refresh_flag():
    disabled_ws = SimpleNamespace(
        ws_scheduled_refresh_enabled=False,
        reset_connections=MagicMock(),
    )
    enabled_ws = SimpleNamespace(
        ws_scheduled_refresh_enabled=True,
        reset_connections=MagicMock(),
    )

    Exchange.ws_connection_reset(SimpleNamespace(_exchange_ws=disabled_ws))
    Exchange.ws_connection_reset(SimpleNamespace(_exchange_ws=enabled_ws))

    disabled_ws.reset_connections.assert_not_called()
    enabled_ws.reset_connections.assert_called_once()


async def test_stats_monitor_skips_refresh_when_scheduled_refresh_disabled(mocker):
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())
    mocker.patch(
        "freqtrade.exchange.exchange_ws.ExchangeWS._create_ws_exchange_pool",
        return_value={"127.0.0.2": ccxt_object, "127.0.0.3": ccxt_object},
    )
    mocker.patch("freqtrade.exchange.exchange_ws.asyncio.sleep", AsyncMock())
    mocker.patch("freqtrade.exchange.exchange_ws.time.time", return_value=20 * 60)

    exchange_ws = ExchangeWS(
        {
            "exchange": {
                "websocket_ip_pool": ["127.0.0.2", "127.0.0.3"],
                "ws_scheduled_refresh_enabled": False,
            }
        },
        ccxt_object,
    )
    exchange_ws._refresh_all_connections = AsyncMock()
    exchange_ws._try_recover_failed_ips = AsyncMock(side_effect=asyncio.CancelledError)
    try:
        with pytest.raises(asyncio.CancelledError):
            await exchange_ws._stats_monitor()
    finally:
        exchange_ws.cleanup()

    exchange_ws._refresh_all_connections.assert_not_called()


async def test_ip_failure_dedupe_counts_one_disconnect_once(mocker):
    config = {
        "exchange": {
            "websocket_ip_pool": ["127.0.0.2"],
            "ws_failure_dedupe_window": 2.0,
            "ws_failure_threshold": 10,
        }
    }
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())
    mocker.patch(
        "freqtrade.exchange.exchange_ws.ExchangeWS._create_ws_exchange_pool",
        return_value={"127.0.0.2": ccxt_object},
    )

    exchange_ws = ExchangeWS(config, ccxt_object)
    time_mock = mocker.patch(
        "freqtrade.exchange.exchange_ws.time.time",
        side_effect=[100.0, 101.0, 104.0, 105.0],
    )
    try:
        await exchange_ws._handle_ip_failure("127.0.0.2", "BTC/USDT:USDT")
        await exchange_ws._handle_ip_failure("127.0.0.2", "BTC/USDT:USDT")
        await exchange_ws._handle_ip_failure("127.0.0.2", "BTC/USDT:USDT")
        await exchange_ws._handle_ip_failure("127.0.0.2", "ETH/USDT:USDT")
    finally:
        exchange_ws.cleanup()

    assert exchange_ws._ip_stats["127.0.0.2"]["failures"] == 3
    assert exchange_ws._ip_consecutive_failures["127.0.0.2"] == 3
    assert time_mock.call_count == 4


async def test_exchangews_get_ohlcv(mocker, caplog):
    config = MagicMock()
    ccxt_object = MagicMock()
    ccxt_object.ohlcvs = {
        "ETH/USDT": {
            "1m": [
                [1635840000000, 100, 200, 300, 400, 500],
                [1635840060000, 101, 201, 301, 401, 501],
                [1635840120000, 102, 202, 302, 402, 502],
            ],
            "5m": [
                [1635840000000, 100, 200, 300, 400, 500],
                [1635840300000, 105, 201, 301, 401, 501],
                [1635840600000, 102, 202, 302, 402, 502],
            ],
        }
    }
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    exchange_ws.klines_last_refresh = {
        ("ETH/USDT", "1m", CandleType.SPOT): 1635840120000,
        ("ETH/USDT", "5m", CandleType.SPOT): 1635840600000,
    }

    # Matching last candle time - drop hint is true
    resp = await exchange_ws.get_ohlcv("ETH/USDT", "1m", CandleType.SPOT, 1635840120000)
    assert resp[0] == "ETH/USDT"
    assert resp[1] == "1m"
    assert resp[3] == [
        [1635840000000, 100, 200, 300, 400, 500],
        [1635840060000, 101, 201, 301, 401, 501],
        [1635840120000, 102, 202, 302, 402, 502],
    ]
    assert resp[4] is True

    # expected time > last candle time - drop hint is false
    resp = await exchange_ws.get_ohlcv("ETH/USDT", "1m", CandleType.SPOT, 1635840180000)
    assert resp[0] == "ETH/USDT"
    assert resp[1] == "1m"
    assert resp[3] == [
        [1635840000000, 100, 200, 300, 400, 500],
        [1635840060000, 101, 201, 301, 401, 501],
        [1635840120000, 102, 202, 302, 402, 502],
    ]
    assert resp[4] is False

    # Change "received" times to be before the candle starts.
    # This should trigger the "time sync" warning.
    exchange_ws.klines_last_refresh = {
        ("ETH/USDT", "1m", CandleType.SPOT): 1635840110000,
        ("ETH/USDT", "5m", CandleType.SPOT): 1635840600000,
    }
    msg = r".*Candle date > last refresh.*"
    assert not log_has_re(msg, caplog)
    resp = await exchange_ws.get_ohlcv("ETH/USDT", "1m", CandleType.SPOT, 1635840120000)
    assert resp[0] == "ETH/USDT"
    assert resp[1] == "1m"
    assert resp[3] == [
        [1635840000000, 100, 200, 300, 400, 500],
        [1635840060000, 101, 201, 301, 401, 501],
        [1635840120000, 102, 202, 302, 402, 502],
    ]
    assert resp[4] is True

    assert log_has_re(msg, caplog)

    exchange_ws.cleanup()
