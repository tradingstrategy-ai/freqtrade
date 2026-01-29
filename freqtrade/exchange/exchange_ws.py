import asyncio
import logging
import time
from copy import deepcopy
from enum import Enum
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


class IPState(Enum):
    """State machine for IP rotation."""
    ACTIVE = "active"           # Handling all streams
    SPINNING_UP = "spinning_up" # Connecting streams, preparing to become active
    STANDBY = "standby"         # Idle but fresh session
    TEARING_DOWN = "tearing_down"  # Disconnecting, moving to standby


class ExchangeWS:
    def __init__(self, config: Config, ccxt_object: ccxt.Exchange) -> None:
        self.config = config
        self._ccxt_object = ccxt_object
        self._background_tasks: set[asyncio.Task] = set()

        self._klines_watching: set[PairWithTimeframe] = set()
        self._klines_scheduled: set[PairWithTimeframe] = set()
        self.klines_last_refresh: dict[PairWithTimeframe, float] = {}
        self.klines_last_request: dict[PairWithTimeframe, float] = {}

        # IP rotation configuration for Rolling Active + Hot Standby
        exchange_config = config.get('exchange', {})
        self._ip_pool: list[str] = exchange_config.get('websocket_ip_pool', [])

        # State tracking for IP rotation
        self._ip_states: dict[str, IPState] = {}
        self._ip_session_times: dict[str, float] = {}  # When session was last refreshed
        self._ip_connection_start: dict[str, float] = {}  # When streams started on this IP
        self._active_ip: str | None = None
        self._rotation_lock: asyncio.Lock | None = None  # Created in event loop

        # Configuration parameters
        self._active_max_age_seconds: int = exchange_config.get('websocket_active_max_age', 1200)  # 20 min
        self._standby_refresh_seconds: int = exchange_config.get('websocket_standby_refresh', 900)  # 15 min
        self._spinup_lead_time_seconds: int = exchange_config.get('websocket_spinup_lead_time', 120)  # 2 min

        # Create dedicated CCXT exchange instances for each IP
        self._ws_exchanges: dict[str, ccxt.Exchange] = {}
        self._ip_stats: dict[str, dict] = {}

        if self._ip_pool:
            self._ws_exchanges = self._create_ws_exchange_pool(ccxt_object)
            # Initialize states: first IP is active, others are standby
            for i, ip in enumerate(self._ip_pool):
                if i == 0:
                    self._ip_states[ip] = IPState.ACTIVE
                    self._active_ip = ip
                else:
                    self._ip_states[ip] = IPState.STANDBY
                self._ip_session_times[ip] = time.time()
                self._ip_stats[ip] = {'active': 0, 'failures': 0, 'last_failure': None}
            logger.info(
                f"[IP-POOL] Initialized Rolling Active + Hot Standby with {len(self._ip_pool)} IPs. "
                f"Active: {self._active_ip}, Standby: {[ip for ip in self._ip_pool if ip != self._active_ip]}"
            )
        else:
            # No IP pool - use single exchange for everything
            self._ws_exchanges = {'default': ccxt_object}
            self._ip_stats['default'] = {'active': 0, 'failures': 0, 'last_failure': None}

        self._thread = Thread(name="ccxt_ws", target=self._start_forever)
        self._thread.start()
        self.__cleanup_called = False

    def _create_single_ws_exchange(self, ip: str) -> ccxt.Exchange:
        """Create a single CCXT exchange instance bound to a specific IP."""
        template = self._ccxt_object
        exchange_class = type(template)

        exchange_config = {
            'apiKey': template.apiKey,
            'secret': template.secret,
            'enableRateLimit': template.enableRateLimit,
            'rateLimit': template.rateLimit,
            'options': {
                **template.options,
                'local_addr': (ip, 0)
            }
        }

        # Add optional credentials if present (for exchanges like Hyperliquid)
        if hasattr(template, 'walletAddress') and template.walletAddress:
            exchange_config['walletAddress'] = template.walletAddress
        if hasattr(template, 'privateKey') and template.privateKey:
            exchange_config['privateKey'] = template.privateKey

        return exchange_class(exchange_config)

    def _create_ws_exchange_pool(self, template: ccxt.Exchange) -> dict[str, ccxt.Exchange]:
        """Create dedicated CCXT exchange instances for each IP in the pool."""
        exchanges = {}
        for ip in self._ip_pool:
            exchanges[ip] = self._create_single_ws_exchange(ip)
            logger.debug(f"[IP-POOL] Created WebSocket exchange for IP {ip}")
        return exchanges

    def _get_ws_exchange_for_pair(self, pair: str) -> tuple[ccxt.Exchange, str]:
        """
        Get the active WebSocket exchange for any pair.
        All pairs go to the active IP (no round-robin distribution).
        """
        if not self._ip_pool or not self._active_ip:
            return self._ws_exchanges.get('default', self._ccxt_object), 'default'

        return self._ws_exchanges[self._active_ip], self._active_ip

    def _log_ip_stats(self) -> None:
        """Log current IP pool statistics."""
        if not self._ip_pool:
            return
        stats_str = " | ".join(
            f"{ip}: state={self._ip_states.get(ip, 'unknown').value}, active={s['active']}, failures={s['failures']}"
            for ip, s in self._ip_stats.items()
        )
        logger.info(f"[IP-STATS] {stats_str}")

    def _log_rotation_state(self) -> None:
        """Log current rotation state for debugging."""
        if not self._ip_pool:
            return
        states = {ip: self._ip_states.get(ip, IPState.STANDBY).value for ip in self._ip_pool}
        ages = {}
        for ip in self._ip_pool:
            if ip in self._ip_connection_start:
                ages[ip] = f"{time.time() - self._ip_connection_start[ip]:.0f}s"
        logger.info(f"[ROTATION-STATE] Active: {self._active_ip} | States: {states} | Ages: {ages}")

    def _start_forever(self) -> None:
        self._loop = asyncio.new_event_loop()

        # Create the rotation lock in the event loop
        self._rotation_lock = asyncio.Lock()

        # Start rotation controller if IP pool is configured
        if self._ip_pool:
            self._loop.create_task(self._rotation_controller())
            logger.info(
                f"[ROTATION-CONTROLLER] Started (max_age={self._active_max_age_seconds}s, "
                f"standby_refresh={self._standby_refresh_seconds}s, lead_time={self._spinup_lead_time_seconds}s)"
            )

        try:
            self._loop.run_forever()
        finally:
            if self._loop.is_running():
                self._loop.stop()

    async def _rotation_controller(self) -> None:
        """
        Main controller for IP rotation.
        Monitors active IP age and triggers rotation.
        Keeps standby IPs fresh.
        """
        while True:
            await asyncio.sleep(30)  # Check every 30 seconds

            if not self._ip_pool:
                continue

            current_time = time.time()
            current_minute = int(current_time // 60) % 60

            # Avoid candle boundaries (:58-:02)
            if current_minute >= 58 or current_minute <= 2:
                logger.debug("[ROTATION] Skipping - candle boundary window")
                continue

            async with self._rotation_lock:
                # 1. Keep standby IPs fresh
                await self._refresh_standby_sessions(current_time)

                # 2. Check if rotation needed
                if self._active_ip and self._ip_states.get(self._active_ip) == IPState.ACTIVE:
                    active_start = self._ip_connection_start.get(self._active_ip)
                    if active_start:
                        active_age = current_time - active_start
                        time_until_rotation = self._active_max_age_seconds - active_age

                        # Log state periodically
                        if int(current_time) % 300 < 30:  # Every ~5 minutes
                            self._log_rotation_state()

                        # Start spinup when we're within lead time of rotation
                        if time_until_rotation <= self._spinup_lead_time_seconds:
                            logger.info(
                                f"[ROTATION] Active IP {self._active_ip} age={active_age:.0f}s, "
                                f"time_until_rotation={time_until_rotation:.0f}s - initiating rotation"
                            )
                            await self._initiate_rotation()

    async def _refresh_standby_sessions(self, current_time: float) -> None:
        """Refresh standby IP sessions to keep them fresh."""
        for ip in self._ip_pool:
            if self._ip_states.get(ip) != IPState.STANDBY:
                continue

            session_age = current_time - self._ip_session_times.get(ip, 0)

            if session_age >= self._standby_refresh_seconds:
                logger.info(f"[STANDBY-REFRESH] Refreshing session for standby IP {ip} (age={session_age:.0f}s)")

                ws_exchange = self._ws_exchanges.get(ip)
                if ws_exchange:
                    try:
                        await ws_exchange.close()
                    except Exception as e:
                        logger.warning(f"[STANDBY-REFRESH] Error closing {ip}: {e}")

                # Recreate exchange instance
                self._ws_exchanges[ip] = self._create_single_ws_exchange(ip)
                self._ip_session_times[ip] = current_time

                logger.info(f"[STANDBY-REFRESH] IP {ip} session refreshed")

    async def _initiate_rotation(self) -> None:
        """Begin rotation to next standby IP."""
        # Find next standby IP
        next_ip = None
        for ip in self._ip_pool:
            if self._ip_states.get(ip) == IPState.STANDBY:
                next_ip = ip
                break

        if not next_ip:
            logger.error("[ROTATION] No standby IP available for rotation!")
            return

        logger.info(f"[ROTATION-START] Initiating rotation: {self._active_ip} -> {next_ip}")

        # Mark next IP as spinning up
        self._ip_states[next_ip] = IPState.SPINNING_UP

        # Start connecting all streams on new IP
        await self._spinup_ip(next_ip)

    async def _spinup_ip(self, ip: str) -> None:
        """Connect all streams on a new IP, then complete rotation."""
        logger.info(f"[SPINUP-START] Connecting all streams on IP {ip}")

        # Get all pairs we should be watching
        pairs_to_connect = set(p[0] for p in self._klines_watching)
        timeframes = set((p[1], p[2]) for p in self._klines_watching)

        ws_exchange = self._ws_exchanges[ip]
        connected_count = 0
        failed_count = 0

        # Connect each pair/timeframe combination
        for pair in pairs_to_connect:
            for timeframe, candle_type in timeframes:
                try:
                    # Initiate watch (this establishes the connection)
                    await ws_exchange.watch_ohlcv(pair, timeframe)
                    connected_count += 1
                except Exception as e:
                    logger.error(f"[SPINUP] Failed to connect {pair}/{timeframe} on {ip}: {e}")
                    failed_count += 1

        logger.info(f"[SPINUP-DONE] Connected {connected_count} streams on IP {ip} ({failed_count} failed)")

        # Record connection start time
        self._ip_connection_start[ip] = time.time()

        # Complete the rotation
        await self._complete_rotation(ip)

    async def _complete_rotation(self, new_active_ip: str) -> None:
        """Complete rotation: switch active, tear down old."""
        old_active_ip = self._active_ip

        logger.info(f"[ROTATION-COMPLETE] Switching active: {old_active_ip} -> {new_active_ip}")

        # Switch active IP
        self._ip_states[new_active_ip] = IPState.ACTIVE
        self._active_ip = new_active_ip

        # Clear scheduled so streams get rescheduled with new exchange
        self._klines_scheduled.clear()

        # Tear down old active
        if old_active_ip and old_active_ip != new_active_ip:
            self._ip_states[old_active_ip] = IPState.TEARING_DOWN
            await self._teardown_ip(old_active_ip)

        self._log_rotation_state()

    async def _teardown_ip(self, ip: str) -> None:
        """Tear down connections on an IP and return it to standby."""
        logger.info(f"[TEARDOWN-START] Disconnecting IP {ip}")

        ws_exchange = self._ws_exchanges.get(ip)
        if ws_exchange:
            try:
                await ws_exchange.close()
                ws_exchange.ohlcvs.clear()
            except Exception as e:
                logger.warning(f"[TEARDOWN] Error closing {ip}: {e}")

        # Recreate fresh exchange instance for standby
        self._ws_exchanges[ip] = self._create_single_ws_exchange(ip)
        self._ip_session_times[ip] = time.time()
        self._ip_connection_start.pop(ip, None)

        # Move to standby
        self._ip_states[ip] = IPState.STANDBY

        # Reset stats
        if ip in self._ip_stats:
            self._ip_stats[ip] = {'active': 0, 'failures': 0, 'last_failure': None}

        logger.info(f"[TEARDOWN-DONE] IP {ip} now in standby")

    async def _handle_unexpected_failure(self, failed_ip: str) -> None:
        """Handle unexpected failure of active IP - immediate failover."""
        if failed_ip != self._active_ip:
            return  # Only care about active IP failures

        logger.warning(f"[FAILOVER] Active IP {failed_ip} failed unexpectedly, initiating failover")

        async with self._rotation_lock:
            # Find any standby IP
            for ip in self._ip_pool:
                if self._ip_states.get(ip) == IPState.STANDBY:
                    logger.info(f"[FAILOVER] Failing over to standby IP {ip}")
                    self._ip_states[ip] = IPState.SPINNING_UP
                    await self._spinup_ip(ip)
                    return

            logger.error("[FAILOVER] No standby IP available! Falling back to REST.")

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

        # Clear from all exchanges (since we rotate)
        for ws_exchange in self._ws_exchanges.values():
            ws_exchange.ohlcvs.get(pair, {}).pop(timeframe, None)

        # Also clear from main exchange
        self._ccxt_object.ohlcvs.get(pair, {}).pop(timeframe, None)
        self.klines_last_refresh.pop(paircomb, None)

    @retrier(retries=3)
    def ohlcvs(self, pair: str, timeframe: str) -> list[list]:
        """
        Returns a copy of the klines for a pair/timeframe combination
        Note: this will only contain the data received from the websocket
            so the data will build up over time.
        """
        try:
            # Get data from active IP's exchange
            if self._active_ip:
                ws_exchange = self._ws_exchanges.get(self._active_ip)
                if ws_exchange:
                    data = deepcopy(ws_exchange.ohlcvs.get(pair, {}).get(timeframe, []))
                    if data:
                        return data

            # Fallback to default/main exchange
            default_exchange = self._ws_exchanges.get('default', self._ccxt_object)
            return deepcopy(default_exchange.ohlcvs.get(pair, {}).get(timeframe, []))
        except RuntimeError as e:
            # Capture runtime errors and retry
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

                # Get the active exchange instance
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
                    # Track connection time for this IP (only set once per rotation cycle)
                    if assigned_ip not in self._ip_connection_start and assigned_ip in self._ip_pool:
                        self._ip_connection_start[assigned_ip] = time.time()
                        logger.info(
                            f"[WS-CONNECTED] First connection on IP {assigned_ip} - tracking rotation timer"
                        )
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

            # Trigger failover if this was the active IP and we have a pool
            if self._ip_pool and assigned_ip == self._active_ip:
                asyncio.create_task(self._handle_unexpected_failure(assigned_ip))
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
        self._klines_watching.add(paircomb)
        self.klines_last_request[paircomb] = dt_ts()
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
