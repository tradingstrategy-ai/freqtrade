import asyncio
import logging
import time
from copy import deepcopy
from datetime import datetime
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
    HOT_BACKUP = "hot_backup"   # Has streams and cached data, ready for instant failover


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

        # Configuration parameters (legacy - kept for backwards compatibility)
        self._active_max_age_seconds: int = exchange_config.get('websocket_active_max_age', 1200)  # 20 min
        self._standby_refresh_seconds: int = exchange_config.get('websocket_standby_refresh', 900)  # 15 min
        self._spinup_lead_time_seconds: int = exchange_config.get('websocket_spinup_lead_time', 120)  # 2 min

        # Danger zone configuration parameters
        self._danger_zone_start_minute: int = exchange_config.get('ws_danger_zone_start', 45)
        self._post_danger_zone_minute: int = exchange_config.get('ws_post_danger_zone', 2)
        self._spinup_schedule: list[int] = exchange_config.get('ws_spinup_schedule', [15, 7])
        self._data_freshness_threshold_ms: int = exchange_config.get('ws_freshness_threshold', 300) * 1000
        self._in_danger_zone: bool = False
        self._spinup_initiated: set[str] = set()  # Track which IPs have been spun up in current danger zone
        self._backup_tasks: dict[str, set[asyncio.Task]] = {}  # Track background tasks per backup IP

        # Create dedicated CCXT exchange instances for each IP
        self._ws_exchanges: dict[str, ccxt.Exchange] = {}
        self._ip_stats: dict[str, dict] = {}

        # Diagnostic metrics per IP for candle boundary analysis
        self._ip_metrics: dict[str, dict] = {}

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
                # Initialize diagnostic metrics for candle boundary analysis
                self._ip_metrics[ip] = {
                    'subscriptions': 0,
                    'messages_received': 0,
                    'messages_sent': 0,
                    'last_minute_reset': time.time(),
                    'candles_received': 0,
                    'last_candle_ts': 0,
                    'errors': [],
                }
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
        danger_zone_str = "IN_DANGER_ZONE" if self._in_danger_zone else "normal"
        logger.info(
            f"[ROTATION-STATE] Active: {self._active_ip} | Zone: {danger_zone_str} | "
            f"States: {states} | Ages: {ages}"
        )

    def _log_metrics_summary(self) -> None:
        """Log comprehensive metrics for all IPs - for candle boundary analysis."""
        if not self._ip_pool:
            return
        for ip in self._ip_pool:
            m = self._ip_metrics.get(ip, {})
            ws_ex = self._ws_exchanges.get(ip)

            # Count actual subscriptions in ccxt
            sub_count = 0
            if ws_ex and hasattr(ws_ex, 'ohlcvs'):
                for pair_data in ws_ex.ohlcvs.values():
                    sub_count += len(pair_data)

            last_candle_str = 'never'
            if m.get('last_candle_ts'):
                last_candle_str = datetime.fromtimestamp(
                    m.get('last_candle_ts', 0) / 1000
                ).strftime('%H:%M:%S')

            logger.info(
                f"[METRICS] IP={ip} state={self._ip_states.get(ip, 'unknown').value if self._ip_states.get(ip) else 'unknown'} "
                f"subscriptions={sub_count} "
                f"candles_received={m.get('candles_received', 0)} "
                f"last_candle={last_candle_str} "
                f"recent_errors={len(m.get('errors', []))}"
            )

    def _log_candle_close_state(self, current_minute: int, current_second: int) -> None:
        """Log state at exact candle close moment (:00:00 - :00:10)."""
        if not self._ip_pool:
            return
        logger.info(f"[CANDLE-CLOSE] At :{current_minute:02d}:{current_second:02d}")
        for ip in self._ip_pool:
            ws_ex = self._ws_exchanges.get(ip)
            if ws_ex:
                # Sample first 3 pairs
                sample_pairs = list(self._klines_watching)[:3]
                for pair, tf, _ in sample_pairs:
                    data = ws_ex.ohlcvs.get(pair, {}).get(tf, [])
                    last_ts = data[-1][0] if data else 0
                    last_str = datetime.fromtimestamp(last_ts / 1000).strftime('%H:%M:%S') if last_ts else 'none'
                    age_sec = (time.time() * 1000 - last_ts) / 1000 if last_ts else -1
                    logger.info(
                        f"[CANDLE-CLOSE] IP={ip} state={self._ip_states.get(ip, 'unknown').value if self._ip_states.get(ip) else 'unknown'} "
                        f"{pair}/{tf} candles={len(data)} "
                        f"last={last_str} age={age_sec:.1f}s"
                    )

    def _start_forever(self) -> None:
        self._loop = asyncio.new_event_loop()

        # Create the rotation lock in the event loop
        self._rotation_lock = asyncio.Lock()

        # Start danger zone controller if IP pool is configured
        if self._ip_pool:
            self._loop.create_task(self._danger_zone_controller())
            logger.info(
                f"[DANGER-ZONE-CONTROLLER] Started (danger_zone_start=:{self._danger_zone_start_minute:02d}, "
                f"post_danger=:{self._post_danger_zone_minute:02d}, spinup_schedule={self._spinup_schedule}, "
                f"freshness_threshold={self._data_freshness_threshold_ms/1000:.0f}s)"
            )

        try:
            self._loop.run_forever()
        finally:
            if self._loop.is_running():
                self._loop.stop()

    async def _danger_zone_controller(self) -> None:
        """
        Manage danger zone entry/exit and scheduled spinups.

        Timeline for 1h candle close at :00:
        - :00-:44  IP1 only (primary)
        - :45      Enter danger zone, IP2 starts spinup
        - :53      IP3 starts spinup
        - :00      DANGER ZONE - Hyperliquid may kill connections
                   ohlcvs() cascades: IP1 → IP2 → IP3 → REST
        - :02      Exit danger zone, determine survivor, teardown others
        """
        while True:
            await asyncio.sleep(10)  # Check every 10 seconds

            if not self._ip_pool:
                continue

            current_time = time.time()
            current_minute = int(current_time // 60) % 60

            async with self._rotation_lock:
                # Enter danger zone at configured minute (default :45)
                if not self._in_danger_zone and current_minute >= self._danger_zone_start_minute:
                    await self._enter_danger_zone(current_minute)

                # Check spinup schedule during danger zone
                if self._in_danger_zone:
                    await self._check_spinup_schedule(current_minute)

                # Exit danger zone at configured minute (default :02)
                # Only exit when minute is >= post_danger AND < 30 (to avoid re-entering)
                if self._in_danger_zone and current_minute >= self._post_danger_zone_minute and current_minute < 30:
                    await self._exit_danger_zone(current_minute)

                # Keep standby IPs fresh (outside danger zone)
                if not self._in_danger_zone:
                    await self._refresh_standby_sessions(current_time)

                # Log state periodically (every ~5 minutes)
                if int(current_time) % 300 < 10:
                    self._log_rotation_state()
                    self._log_metrics_summary()

                # DIAGNOSTIC: Log at exact :00 boundary (:00:00 - :00:10)
                current_second = int(current_time) % 60
                if current_minute == 0 and current_second < 10:
                    self._log_candle_close_state(current_minute, current_second)

    async def _enter_danger_zone(self, current_minute: int) -> None:
        """Enter danger zone - prepare for candle boundary."""
        self._in_danger_zone = True
        self._spinup_initiated.clear()  # Reset spinup tracking for new danger zone
        logger.info(
            f"[DANGER-ZONE-ENTER] Entering danger zone at :{current_minute:02d}. "
            f"Active IP: {self._active_ip}. Schedule: spinup at {self._spinup_schedule} min before :00"
        )

    async def _check_spinup_schedule(self, current_minute: int) -> None:
        """Spin up backup IPs according to schedule."""
        standby_ips = [ip for ip in self._ip_pool if self._ip_states.get(ip) == IPState.STANDBY]

        # Schedule: [15, 7] means IP2 at :45 (60-15), IP3 at :53 (60-7)
        for i, minutes_before in enumerate(self._spinup_schedule):
            target_minute = 60 - minutes_before
            if current_minute >= target_minute and i < len(standby_ips):
                ip = standby_ips[i]
                if ip not in self._spinup_initiated:
                    logger.info(
                        f"[SPINUP-SCHEDULE] Minute :{current_minute:02d} >= :{target_minute:02d}, "
                        f"spinning up backup #{i+1}: {ip}"
                    )
                    self._spinup_initiated.add(ip)
                    await self._spinup_for_danger_zone(ip)

    async def _spinup_for_danger_zone(self, ip: str) -> None:
        """Spin up IP with continuous watch loops to populate ohlcvs cache."""
        logger.info(f"[SPINUP-START] Starting HOT_BACKUP spinup for IP {ip}")
        self._ip_states[ip] = IPState.SPINNING_UP

        # Get all pairs we should be watching
        pairs_to_watch = list(self._klines_watching)

        if not pairs_to_watch:
            logger.warning(f"[SPINUP] No pairs in _klines_watching, cannot spin up backup {ip}")
            self._ip_states[ip] = IPState.STANDBY
            return

        ws_exchange = self._ws_exchanges[ip]
        self._backup_tasks[ip] = set()

        # Start continuous watch tasks for each pair (like active IP does)
        for pair, timeframe, candle_type in pairs_to_watch:
            task = asyncio.create_task(
                self._continuously_watch_backup(pair, timeframe, candle_type, ws_exchange, ip)
            )
            self._backup_tasks[ip].add(task)

        # Record connection start time
        self._ip_connection_start[ip] = time.time()

        # Mark as HOT_BACKUP - NOT ACTIVE (just ready for failover)
        self._ip_states[ip] = IPState.HOT_BACKUP

        logger.info(
            f"[SPINUP-DONE] IP {ip} now HOT_BACKUP with {len(pairs_to_watch)} watch tasks started"
        )

    async def _continuously_watch_backup(
        self, pair: str, timeframe: str, candle_type: CandleType,
        ws_exchange: ccxt.Exchange, ip: str
    ) -> None:
        """Continuous watch loop for backup IP to populate ohlcvs cache."""
        first_message_received = False
        try:
            while self._ip_states.get(ip) in (IPState.SPINNING_UP, IPState.HOT_BACKUP):
                data = await ws_exchange.watch_ohlcv(pair, timeframe)

                # DIAGNOSTIC: Log every candle update on backup IPs
                if data:
                    last_ts = data[-1][0]
                    if ip in self._ip_metrics:
                        self._ip_metrics[ip]['candles_received'] += 1
                        self._ip_metrics[ip]['last_candle_ts'] = last_ts
                    current_minute = int(time.time() // 60) % 60
                    # Log at debug level normally, but info near candle boundaries
                    if current_minute >= 58 or current_minute <= 2:
                        logger.info(
                            f"[BACKUP-CANDLE] IP={ip} {pair}/{timeframe} "
                            f"candles={len(data)} last_ts={last_ts} "
                            f"({datetime.fromtimestamp(last_ts/1000).strftime('%H:%M:%S')}) "
                            f"minute=:{current_minute:02d}"
                        )

                if not first_message_received:
                    first_message_received = True
                    logger.info(
                        f"[BACKUP-CONNECTED] First data for {pair}/{timeframe} on backup IP {ip} "
                        f"(data points: {len(data)})"
                    )
        except ccxt.ExchangeClosedByUser:
            pass  # Expected during teardown
        except ccxt.BaseError as e:
            # DIAGNOSTIC: Log connection errors with full context
            error_time = datetime.now().strftime('%H:%M:%S.%f')
            current_minute = int(time.time() // 60) % 60
            logger.error(
                f"[WS-CONN-ERROR] :{current_minute:02d} IP={ip} {pair}/{timeframe} "
                f"error={type(e).__name__}: {str(e)[:200]} "
                f"state={self._ip_states.get(ip, 'unknown').value if self._ip_states.get(ip) else 'unknown'} "
                f"at={error_time}"
            )
            # Track in metrics
            if ip in self._ip_metrics:
                self._ip_metrics[ip]['errors'].append({
                    'time': error_time,
                    'minute': current_minute,
                    'pair': pair,
                    'error': str(e)[:100]
                })
                self._ip_metrics[ip]['errors'] = self._ip_metrics[ip]['errors'][-10:]  # Keep last 10

    async def _exit_danger_zone(self, current_minute: int) -> None:
        """Exit danger zone - determine survivor and teardown others."""
        logger.info(f"[DANGER-ZONE-EXIT] Exiting danger zone at :{current_minute:02d}")

        # Determine which IP has fresh data
        survivor = await self._determine_survivor()

        if survivor and survivor != self._active_ip:
            logger.info(
                f"[DANGER-ZONE-EXIT] Survivor changed: {self._active_ip} -> {survivor}"
            )
            await self._promote_to_active(survivor)
        else:
            logger.info(f"[DANGER-ZONE-EXIT] Active IP {self._active_ip} survived")

        # Teardown non-survivor HOT_BACKUP IPs
        for ip in self._ip_pool:
            if ip != self._active_ip and self._ip_states.get(ip) == IPState.HOT_BACKUP:
                logger.info(f"[TEARDOWN] Tearing down non-survivor HOT_BACKUP IP {ip}")
                await self._teardown_ip(ip)

        self._in_danger_zone = False
        self._spinup_initiated.clear()
        self._log_rotation_state()

    async def _determine_survivor(self) -> str | None:
        """Find IP with fresh data after danger zone."""
        # Check IPs in priority order: ACTIVE first, then HOT_BACKUPs
        ips_to_check = []
        if self._active_ip:
            ips_to_check.append(self._active_ip)
        for ip in self._ip_pool:
            if ip not in ips_to_check and self._ip_states.get(ip) == IPState.HOT_BACKUP:
                ips_to_check.append(ip)

        for ip in ips_to_check:
            ws_exchange = self._ws_exchanges.get(ip)
            if not ws_exchange:
                continue

            # Sample a few pairs to check freshness
            sample_pairs = list(self._klines_watching)[:3]
            fresh_count = 0
            for p, tf, _ in sample_pairs:
                data = ws_exchange.ohlcvs.get(p, {}).get(tf, [])
                if self._is_data_fresh(data):
                    fresh_count += 1

            if fresh_count > 0:
                logger.debug(f"[SURVIVOR] IP {ip} has {fresh_count} fresh pairs")
                return ip
            else:
                logger.warning(f"[SURVIVOR] IP {ip} has no fresh data")

        logger.warning("[SURVIVOR] No IP with fresh data found, returning current active")
        return self._active_ip  # Fallback to current active

    async def _promote_to_active(self, ip: str) -> None:
        """Promote IP to active status."""
        old_active = self._active_ip

        self._ip_states[ip] = IPState.ACTIVE
        self._active_ip = ip
        self._klines_scheduled.clear()

        logger.info(f"[PROMOTE] IP {ip} promoted to ACTIVE (was: {old_active})")

        if old_active and old_active != ip:
            # Demote old active to HOT_BACKUP temporarily (will be torn down in _exit_danger_zone)
            self._ip_states[old_active] = IPState.HOT_BACKUP

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

        # Reschedule all pairs from _klines_watching on the new active IP
        logger.info(
            f"[ROTATION-RESCHEDULE] Rescheduling {len(self._klines_watching)} pairs on new active IP {new_active_ip}"
        )
        await self._schedule_while_true()

        # Tear down old active
        if old_active_ip and old_active_ip != new_active_ip:
            self._ip_states[old_active_ip] = IPState.TEARING_DOWN
            await self._teardown_ip(old_active_ip)

        self._log_rotation_state()

    async def _teardown_ip(self, ip: str) -> None:
        """Tear down connections on an IP and return it to standby."""
        logger.info(f"[TEARDOWN-START] Disconnecting IP {ip}")

        # Cancel any backup watch tasks for this IP
        if ip in self._backup_tasks:
            for task in self._backup_tasks[ip]:
                task.cancel()
            self._backup_tasks.pop(ip, None)
            logger.debug(f"[TEARDOWN] Cancelled backup tasks for IP {ip}")

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
            # First, check for HOT_BACKUP IPs (already have data, instant failover)
            for ip in self._ip_pool:
                if self._ip_states.get(ip) == IPState.HOT_BACKUP:
                    logger.info(f"[FAILOVER] Instant failover to HOT_BACKUP IP {ip}")
                    await self._promote_to_active(ip)
                    return

            # Fallback: spin up a standby IP
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

    def _get_ips_by_priority(self) -> list[str]:
        """Return IPs ordered: ACTIVE first, then HOT_BACKUPs, then SPINNING_UP."""
        result = []
        if self._active_ip:
            result.append(self._active_ip)
        for ip in self._ip_pool:
            if ip not in result and self._ip_states.get(ip) == IPState.HOT_BACKUP:
                result.append(ip)
        for ip in self._ip_pool:
            if ip not in result and self._ip_states.get(ip) == IPState.SPINNING_UP:
                result.append(ip)
        return result

    def _is_data_fresh(self, data: list) -> bool:
        """Check if data is fresh enough to use."""
        if not data:
            return False
        last_ts = data[-1][0]  # Timestamp in ms
        current_ts = time.time() * 1000
        return (current_ts - last_ts) < self._data_freshness_threshold_ms

    @retrier(retries=3)
    def ohlcvs(self, pair: str, timeframe: str) -> list[list]:
        """
        Returns a copy of the klines for a pair/timeframe combination.
        Cascades through all IPs with data before returning empty.

        Priority order: ACTIVE → HOT_BACKUP → SPINNING_UP → REST fallback

        Retry strategy at candle boundaries:
        - Check 1: Immediate (at :00:01)
        - Check 2: After 3s (at :00:04) if all stale
        - Check 3: After 3s more (at :00:07) if still stale
        - Check 4: After 4s more (at :00:11) if still stale
        - Give up: Return best available data or empty (REST fallback)
        """
        try:
            current_minute = int(time.time() // 60) % 60
            near_boundary = current_minute >= 58 or current_minute <= 2

            # Retry configuration: delays in seconds for each retry
            retry_delays = [3, 3, 4]  # Total: 10 seconds max wait
            retry_attempt = 0
            max_retries = len(retry_delays)

            while retry_attempt <= max_retries:
                # DIAGNOSTIC: Log ALL IPs' data state near candle boundary
                if near_boundary and self._ip_pool and retry_attempt == 0:
                    for ip in self._ip_pool:
                        ws_ex = self._ws_exchanges.get(ip)
                        if ws_ex:
                            data = ws_ex.ohlcvs.get(pair, {}).get(timeframe, [])
                            last_ts = data[-1][0] if data else 0
                            age_sec = (time.time() * 1000 - last_ts) / 1000 if last_ts else -1
                            logger.info(
                                f"[OHLCV-CHECK] :{current_minute:02d} IP={ip} "
                                f"state={self._ip_states.get(ip, 'unknown').value if self._ip_states.get(ip) else 'unknown'} "
                                f"{pair} candles={len(data)} last_ts={last_ts} age={age_sec:.1f}s "
                                f"fresh={self._is_data_fresh(data)}"
                            )

                # Cascade through IPs in priority order
                found_fresh = False
                for ip in self._get_ips_by_priority():
                    ws_exchange = self._ws_exchanges.get(ip)
                    if not ws_exchange:
                        continue

                    data = ws_exchange.ohlcvs.get(pair, {}).get(timeframe, [])
                    if data and self._is_data_fresh(data):
                        if ip != self._active_ip:
                            logger.info(
                                f"[CASCADE] Using data from {ip} (not active) for {pair}/{timeframe}"
                            )
                        if retry_attempt > 0:
                            logger.info(
                                f"[CASCADE-RETRY-SUCCESS] Fresh data found after {retry_attempt} retries "
                                f"for {pair}/{timeframe} on IP {ip}"
                            )
                        return deepcopy(data)

                # No fresh data found in this attempt
                if retry_attempt < max_retries and near_boundary and self._ip_pool:
                    retry_delay = retry_delays[retry_attempt]
                    retry_attempt += 1
                    logger.info(
                        f"[CASCADE-RETRY] No fresh data for {pair}/{timeframe}, "
                        f"retry {retry_attempt}/{max_retries} after {retry_delay}s"
                    )
                    time.sleep(retry_delay)
                    # Loop will retry cascade
                else:
                    # Max retries reached or not near boundary - stop retrying
                    break

            # All retries exhausted - return best available data as fallback
            if near_boundary and self._ip_pool:
                logger.warning(
                    f"[CASCADE-RETRY-EXHAUSTED] All retries exhausted for {pair}/{timeframe}, "
                    f"returning best available data or empty"
                )
                # Try to return stale data as last resort
                for ip in self._get_ips_by_priority():
                    ws_exchange = self._ws_exchanges.get(ip)
                    if ws_exchange:
                        data = ws_exchange.ohlcvs.get(pair, {}).get(timeframe, [])
                        if data:
                            logger.warning(
                                f"[CASCADE-STALE-FALLBACK] Using stale data from {ip} "
                                f"for {pair}/{timeframe} (age={(time.time() * 1000 - data[-1][0])/1000:.1f}s)"
                            )
                            return deepcopy(data)

            # Fallback to default/main exchange (no IP pool case)
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
            # DIAGNOSTIC: Log connection errors with full context
            error_time = datetime.now().strftime('%H:%M:%S.%f')
            current_minute = int(time.time() // 60) % 60

            # Track failure statistics
            if assigned_ip in self._ip_stats:
                self._ip_stats[assigned_ip]['failures'] += 1
                self._ip_stats[assigned_ip]['last_failure'] = str(e)[:100]

            logger.error(
                f"[WS-CONN-ERROR] :{current_minute:02d} IP={assigned_ip} {pair}/{timeframe} "
                f"error={type(e).__name__}: {str(e)[:200]} "
                f"state={self._ip_states.get(assigned_ip, 'unknown').value if self._ip_states.get(assigned_ip) else 'unknown'} "
                f"at={error_time}"
            )

            # Track in metrics
            if assigned_ip in self._ip_metrics:
                self._ip_metrics[assigned_ip]['errors'].append({
                    'time': error_time,
                    'minute': current_minute,
                    'pair': pair,
                    'error': str(e)[:100]
                })
                self._ip_metrics[assigned_ip]['errors'] = self._ip_metrics[assigned_ip]['errors'][-10:]

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
            # NOTE: Do NOT remove from _klines_watching here!
            # _klines_watching represents the desired state (what we want to watch).
            # When a task exits due to IP failure, we still want to watch this pair
            # on the new active IP. Only cleanup_expired() should remove from _klines_watching.
            # self._klines_watching.discard((pair, timeframe, candle_type))  # REMOVED

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
