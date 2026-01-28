import asyncio
import logging
import time
from copy import deepcopy
from functools import partial
from threading import Thread

import ccxt

from freqtrade.constants import Config, PairWithTimeframe
from freqtrade.enums.candletype import CandleType
from freqtrade.exceptions import TemporaryError
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange import timeframe_to_seconds
from freqtrade.exchange.exchange_types import OHLCVResponse
from freqtrade.util import dt_ts, format_ms_time, format_ms_time_det


logger = logging.getLogger(__name__)


class ExchangeWS:
    def __init__(self, config: Config, ccxt_object: ccxt.Exchange) -> None:
        self.config = config
        self._ccxt_object = ccxt_object
        self._background_tasks: set[asyncio.Task] = set()

        self._klines_watching: set[PairWithTimeframe] = set()
        self._klines_scheduled: set[PairWithTimeframe] = set()
        self.klines_last_refresh: dict[PairWithTimeframe, float] = {}
        self.klines_last_request: dict[PairWithTimeframe, float] = {}

        # IP rotation configuration for distributing WebSocket connections across multiple IPs
        exchange_config = config.get('exchange', {})
        self._ip_pool: list[str] = exchange_config.get('websocket_ip_pool', [])
        self._current_ip_index = 0
        self._ip_assignments: dict[str, str] = {}  # Track which pair uses which IP

        # Create dedicated CCXT exchange instances for each IP in the pool
        # Each instance has its own session/connector with a fixed local_addr
        self._ws_exchanges: dict[str, ccxt.Exchange] = {}

        # IP pool statistics for debugging
        self._ip_stats: dict[str, dict] = {}  # {ip: {active: int, failures: int, last_failure: str}}

        if self._ip_pool:
            self._ws_exchanges = self._create_ws_exchange_pool(ccxt_object)
            # Initialize stats for each IP
            for ip in self._ip_pool:
                self._ip_stats[ip] = {'active': 0, 'failures': 0, 'last_failure': None}
            logger.info(f"WebSocket IP pool initialized with {len(self._ws_exchanges)} exchanges: {self._ip_pool}")
        else:
            # No IP pool - use single exchange for everything
            self._ws_exchanges = {'default': ccxt_object}
            self._ip_stats['default'] = {'active': 0, 'failures': 0, 'last_failure': None}

        self._thread = Thread(name="ccxt_ws", target=self._start_forever)
        self._thread.start()
        self.__cleanup_called = False

    def _get_ip_for_exchange(self, ws_exchange: ccxt.Exchange) -> str:
        """Reverse lookup: find which IP an exchange instance is bound to."""
        for ip, exchange in self._ws_exchanges.items():
            if exchange is ws_exchange:
                return ip
        return 'unknown'

    def _log_ip_stats(self) -> None:
        """Log current IP pool statistics."""
        if not self._ip_pool:
            return
        stats_str = " | ".join(
            f"{ip}: active={s['active']}, failures={s['failures']}"
            for ip, s in self._ip_stats.items()
        )
        logger.info(f"[IP-STATS] {stats_str}")

    def _create_ws_exchange_pool(self, template: ccxt.Exchange) -> dict[str, ccxt.Exchange]:
        """Create dedicated CCXT exchange instances for each IP in the pool."""
        exchanges = {}
        exchange_class = type(template)  # e.g., ccxt.pro.hyperliquid

        for ip in self._ip_pool:
            # Build config dict with credentials from template
            exchange_config = {
                'apiKey': template.apiKey,
                'secret': template.secret,
                'enableRateLimit': template.enableRateLimit,
                'rateLimit': template.rateLimit,
                'options': {
                    **template.options,
                    'local_addr': (ip, 0)  # Set BEFORE first connection
                }
            }

            # Add optional credentials if present (for exchanges like Hyperliquid)
            if hasattr(template, 'walletAddress') and template.walletAddress:
                exchange_config['walletAddress'] = template.walletAddress
            if hasattr(template, 'privateKey') and template.privateKey:
                exchange_config['privateKey'] = template.privateKey

            new_exchange = exchange_class(exchange_config)
            exchanges[ip] = new_exchange
            logger.debug(f"Created WebSocket exchange for IP {ip}")

        return exchanges

    def _start_forever(self) -> None:
        self._loop = asyncio.new_event_loop()
        try:
            self._loop.run_forever()
        finally:
            if self._loop.is_running():
                self._loop.stop()

    def _get_ws_exchange_for_pair(self, pair: str) -> tuple[ccxt.Exchange, str]:
        """
        Get the WebSocket exchange instance for a pair (round-robin assignment).
        Returns tuple of (dedicated CCXT exchange instance, IP address).
        """
        if not self._ip_pool:
            return self._ws_exchanges.get('default', self._ccxt_object), 'default'

        # Check if pair already has an IP assigned
        if pair not in self._ip_assignments:
            # Assign next IP in round-robin fashion
            assigned_ip = self._ip_pool[self._current_ip_index]
            self._ip_assignments[pair] = assigned_ip
            self._current_ip_index = (self._current_ip_index + 1) % len(self._ip_pool)

            # Count connections on this IP
            connections_on_ip = sum(1 for ip in self._ip_assignments.values() if ip == assigned_ip)
            logger.info(
                f"[IP-ASSIGN] NEW: {pair} -> IP {assigned_ip} "
                f"({connections_on_ip} pairs on this IP)"
            )

        assigned_ip = self._ip_assignments[pair]
        return self._ws_exchanges[assigned_ip], assigned_ip

    def cleanup(self) -> None:
        logger.debug("Cleanup called - stopping")
        self._klines_watching.clear()
        for task in self._background_tasks:
            task.cancel()
        if hasattr(self, "_loop") and not self._loop.is_closed():
            self.reset_connections()

            self._loop.call_soon_threadsafe(self._loop.stop)
            time.sleep(0.1)
            if not self._loop.is_closed():
                self._loop.close()

        self._thread.join()
        logger.debug("Stopped")

    def reset_connections(self) -> None:
        """
        Reset all connections - avoids "connection-reset" errors that happen after ~9 days
        """
        if hasattr(self, "_loop") and not self._loop.is_closed():
            logger.info("Resetting WS connections.")
            asyncio.run_coroutine_threadsafe(self._cleanup_async(), loop=self._loop)
            while not self.__cleanup_called:
                time.sleep(0.1)
        self.__cleanup_called = False

    async def _cleanup_async(self) -> None:
        try:
            # Close all WebSocket exchanges
            for ip, ws_exchange in self._ws_exchanges.items():
                try:
                    await ws_exchange.close()
                    # Clear the cache.
                    # Not doing this will cause problems on startup with dynamic pairlists
                    ws_exchange.ohlcvs.clear()
                    logger.debug(f"Closed WebSocket exchange for IP {ip}")
                except Exception:
                    logger.exception(f"Exception closing exchange for IP {ip}")

            # Also close main exchange if not in ws_exchanges
            if self._ccxt_object not in self._ws_exchanges.values():
                await self._ccxt_object.close()
                self._ccxt_object.ohlcvs.clear()
        except Exception:
            logger.exception("Exception in _cleanup_async")
        finally:
            self.__cleanup_called = True

    def _pop_history(self, paircomb: PairWithTimeframe) -> None:
        """
        Remove history for a pair/timeframe combination from ccxt cache
        """
        pair = paircomb[0]
        timeframe = paircomb[1]

        # Clear from assigned exchange
        if pair in self._ip_assignments:
            ip = self._ip_assignments[pair]
            ws_exchange = self._ws_exchanges.get(ip)
            if ws_exchange:
                ws_exchange.ohlcvs.get(pair, {}).pop(timeframe, None)

        # Also try default/main exchange as fallback
        default_exchange = self._ws_exchanges.get('default', self._ccxt_object)
        default_exchange.ohlcvs.get(pair, {}).pop(timeframe, None)
        self.klines_last_refresh.pop(paircomb, None)

    @retrier(retries=3)
    def ohlcvs(self, pair: str, timeframe: str) -> list[list]:
        """
        Returns a copy of the klines for a pair/timeframe combination
        Note: this will only contain the data received from the websocket
            so the data will build up over time.
        """
        try:
            # Find the exchange that has this pair's data
            if pair in self._ip_assignments:
                ip = self._ip_assignments[pair]
                ws_exchange = self._ws_exchanges.get(ip)
                if ws_exchange:
                    data = deepcopy(ws_exchange.ohlcvs.get(pair, {}).get(timeframe, []))
                    logger.debug(f"[WS-DATA] Read {len(data)} candles for {pair}/{timeframe} from IP {ip}")
                    return data
            # Fallback to default/main exchange
            default_exchange = self._ws_exchanges.get('default', self._ccxt_object)
            data = deepcopy(default_exchange.ohlcvs.get(pair, {}).get(timeframe, []))
            logger.debug(f"[WS-DATA] Read {len(data)} candles for {pair}/{timeframe} from default exchange")
            return data
        except RuntimeError as e:
            # Capture runtime errors and retry
            # TemporaryError does not cause backoff - so we're essentially retrying immediately
            raise TemporaryError(f"Error deepcopying: {e}") from e

    def cleanup_expired(self) -> None:
        """
        Remove pairs from watchlist if they've not been requested within
        the last timeframe (+ offset)
        """
        changed = False
        for p in list(self._klines_watching):
            _, timeframe, _ = p
            timeframe_s = timeframe_to_seconds(timeframe)
            last_refresh = self.klines_last_request.get(p, 0)
            if last_refresh > 0 and (dt_ts() - last_refresh) > ((timeframe_s + 20) * 1000):
                logger.info(f"Removing {p} from websocket watchlist.")
                self._klines_watching.discard(p)
                # Pop history to avoid getting stale data
                self._pop_history(p)
                changed = True
        if changed:
            logger.info(f"Removal done: new watch list ({len(self._klines_watching)})")

    async def _schedule_while_true(self) -> None:
        # For the ones we should be watching
        for p in self._klines_watching:
            # Check if they're already scheduled
            if p not in self._klines_scheduled:
                self._klines_scheduled.add(p)
                pair, timeframe, candle_type = p

                # Get the dedicated exchange instance for this pair
                ws_exchange, assigned_ip = self._get_ws_exchange_for_pair(pair)

                # Track active connections per IP
                if assigned_ip in self._ip_stats:
                    self._ip_stats[assigned_ip]['active'] += 1

                logger.info(
                    f"[WS-SCHEDULE] {pair}/{timeframe} -> IP {assigned_ip} "
                    f"(active on IP: {self._ip_stats.get(assigned_ip, {}).get('active', '?')})"
                )

                task = asyncio.create_task(
                    self._continuously_async_watch_ohlcv(pair, timeframe, candle_type, ws_exchange, assigned_ip)
                )
                self._background_tasks.add(task)
                task.add_done_callback(
                    partial(
                        self._continuous_stopped,
                        pair=pair,
                        timeframe=timeframe,
                        candle_type=candle_type,
                        assigned_ip=assigned_ip,
                    )
                )

    async def _unwatch_ohlcv(self, pair: str, timeframe: str, candle_type: CandleType) -> None:
        try:
            ws_exchange, assigned_ip = self._get_ws_exchange_for_pair(pair)
            logger.debug(f"[WS-UNWATCH] {pair}/{timeframe} on IP {assigned_ip}")
            await ws_exchange.un_watch_ohlcv_for_symbols([[pair, timeframe]])
        except ccxt.NotSupported as e:
            logger.debug("un_watch_ohlcv_for_symbols not supported: %s", e)
        except Exception:
            logger.exception("Exception in _unwatch_ohlcv")

    def _continuous_stopped(
        self, task: asyncio.Task, pair: str, timeframe: str, candle_type: CandleType,
        assigned_ip: str = 'unknown'
    ):
        self._background_tasks.discard(task)

        # Decrement active count for this IP
        if assigned_ip in self._ip_stats:
            self._ip_stats[assigned_ip]['active'] = max(0, self._ip_stats[assigned_ip]['active'] - 1)

        result = "done"
        if task.cancelled():
            result = "cancelled"
        else:
            if (result1 := task.result()) is not None:
                result = str(result1)

        logger.info(
            f"[WS-TASK-DONE] {pair}/{timeframe} on IP {assigned_ip} - result: {result} "
            f"(remaining active on IP: {self._ip_stats.get(assigned_ip, {}).get('active', '?')})"
        )
        asyncio.run_coroutine_threadsafe(
            self._unwatch_ohlcv(pair, timeframe, candle_type), loop=self._loop
        )

        self._klines_scheduled.discard((pair, timeframe, candle_type))
        self._pop_history((pair, timeframe, candle_type))

    async def _continuously_async_watch_ohlcv(
        self, pair: str, timeframe: str, candle_type: CandleType,
        ws_exchange: ccxt.Exchange, assigned_ip: str
    ) -> None:
        first_message_received = False
        try:
            logger.info(f"[WS-CONNECT] Starting watch for {pair}/{timeframe} on IP {assigned_ip}")
            while (pair, timeframe, candle_type) in self._klines_watching:
                start = dt_ts()
                data = await ws_exchange.watch_ohlcv(pair, timeframe)
                self.klines_last_refresh[(pair, timeframe, candle_type)] = dt_ts()

                if not first_message_received:
                    first_message_received = True
                    logger.info(
                        f"[WS-CONNECTED] First data received for {pair}/{timeframe} on IP {assigned_ip} "
                        f"(data points: {len(data)})"
                    )

                logger.debug(
                    f"watch done {pair}, {timeframe}, IP {assigned_ip}, data {len(data)} "
                    f"in {(dt_ts() - start) / 1000:.3f}s"
                )
        except ccxt.ExchangeClosedByUser:
            logger.info(f"[WS-CLOSED] Exchange closed by user for {pair}/{timeframe} on IP {assigned_ip}")
        except ccxt.BaseError as e:
            # Track failure statistics
            if assigned_ip in self._ip_stats:
                self._ip_stats[assigned_ip]['failures'] += 1
                self._ip_stats[assigned_ip]['last_failure'] = str(e)[:100]

            logger.error(
                f"[WS-ERROR] {pair}/{timeframe} on IP {assigned_ip} failed: {type(e).__name__}: {e}"
            )
            # Log IP stats after failure
            self._log_ip_stats()
        finally:
            logger.info(
                f"[WS-STOPPED] Watch ended for {pair}/{timeframe} on IP {assigned_ip} "
                f"(received_data: {first_message_received})"
            )
            self._klines_watching.discard((pair, timeframe, candle_type))

    def schedule_ohlcv(self, pair: str, timeframe: str, candle_type: CandleType) -> None:
        """
        Schedule a pair/timeframe combination to be watched
        """
        paircomb = (pair, timeframe, candle_type)
        was_watching = paircomb in self._klines_watching
        was_scheduled = paircomb in self._klines_scheduled

        self._klines_watching.add(paircomb)
        self.klines_last_request[paircomb] = dt_ts()

        # Log if this is a reschedule attempt (was not watching but has IP assignment)
        if not was_watching and not was_scheduled and pair in self._ip_assignments:
            assigned_ip = self._ip_assignments[pair]
            logger.info(
                f"[WS-RESCHEDULE] {pair}/{timeframe} will be rescheduled on IP {assigned_ip} "
                f"(IP failures: {self._ip_stats.get(assigned_ip, {}).get('failures', '?')})"
            )

        asyncio.run_coroutine_threadsafe(self._schedule_while_true(), loop=self._loop)
        self.cleanup_expired()

    async def get_ohlcv(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        candle_ts: int,
    ) -> OHLCVResponse:
        """
        Returns cached klines from ccxt's "watch" cache.
        :param candle_ts: timestamp of the end-time of the candle we expect.
        """
        # Deepcopy the response - as it might be modified in the background as new messages arrive
        candles = self.ohlcvs(pair, timeframe)
        refresh_date = self.klines_last_refresh[(pair, timeframe, candle_type)]
        received_ts = candles[-1][0] if candles else 0
        drop_hint = received_ts >= candle_ts
        if received_ts > refresh_date:
            logger.warning(
                f"{pair}, {timeframe} - Candle date > last refresh "
                f"({format_ms_time(received_ts)} > {format_ms_time_det(refresh_date)}). "
                "This usually suggests a problem with time synchronization."
            )
        logger.debug(
            f"watch result for {pair}, {timeframe} with length {len(candles)}, "
            f"r_ts={format_ms_time(received_ts)}, "
            f"lref={format_ms_time_det(refresh_date)}, "
            f"candle_ts={format_ms_time(candle_ts)}, {drop_hint=}"
        )
        return pair, timeframe, candle_type, candles, drop_hint
