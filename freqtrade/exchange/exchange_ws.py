import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime
from enum import Enum
from functools import partial
from threading import Thread

import aiohttp
import ccxt

from freqtrade.constants import Config, ExchangeConfig, PairWithTimeframe
from freqtrade.enums.candletype import CandleType
from freqtrade.exceptions import TemporaryError
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange import timeframe_to_seconds
from freqtrade.exchange.exchange_types import OHLCVResponse
from freqtrade.util import dt_ts, format_ms_time, format_ms_time_det


logger = logging.getLogger(__name__)


class IPState(Enum):
    """State machine for IP distribution."""
    ACTIVE = "active"           # Handling assigned streams
    FAILED = "failed"           # IP has failed, pairs reassigned to others


class ExchangeWS:
    def __init__(
        self,
        config: Config,
        ccxt_object: ccxt.Exchange,
        exchange_config: ExchangeConfig | None = None,
        wallet_address: str = '',
    ) -> None:
        self.config = config
        self._ccxt_object = ccxt_object
        self._background_tasks: set[asyncio.Task] = set()

        self._klines_watching: set[PairWithTimeframe] = set()
        self._klines_scheduled: set[PairWithTimeframe] = set()
        self.klines_last_refresh: dict[PairWithTimeframe, float] = {}
        self.klines_last_request: dict[PairWithTimeframe, float] = {}

        # IP distribution configuration - pairs distributed across IP pool
        # Use explicit exchange_config if provided (preserves credentials), otherwise fallback
        exchange_config = (
            exchange_config if exchange_config is not None
            else config.get('exchange', {})
        )
        self._ip_pool: list[str] = exchange_config.get('websocket_ip_pool', [])

        # State tracking for IP distribution
        self._ip_states: dict[str, IPState] = {}
        self._ip_session_times: dict[str, float] = {}  # When session was last refreshed
        self._ip_connection_start: dict[str, float] = {}  # When streams started on this IP

        # IP failure tracking for recovery mechanism
        self._ip_failure_time: dict[str, float] = {}  # When IP was marked FAILED
        self._ip_consecutive_failures: dict[str, int] = {}  # Consecutive failure count per IP

        # Exponential backoff tracking per IP for reconnection
        self._ip_backoff_delay: dict[str, float] = {}  # Current backoff delay per IP

        # Desired subscriptions for auto-resubscribe after recovery
        self._desired_subscriptions: set[PairWithTimeframe] = set()

        # Pair-to-IP assignment cache for consistent routing
        self._pair_ip_assignment: dict[str, str] = {}

        # Track last refresh time to avoid duplicate refreshes
        self._last_periodic_refresh: float = 0

        # Data freshness threshold for checking stale data
        self._data_freshness_threshold_ms: int = (
            exchange_config.get('ws_freshness_threshold', 300) * 1000
        )

        # Configurable failure thresholds
        self._failure_threshold: int = exchange_config.get('ws_failure_threshold', 3)
        self._recovery_cooldown: int = exchange_config.get('ws_recovery_cooldown', 300)
        self._backoff_max: float = exchange_config.get('ws_backoff_max', 30.0)

        # Per-IP rate limit tracking (1200 weight/minute budget per Hyperliquid docs)
        self._ip_weight_budget: int = 1200  # Per-IP per-minute limit
        self._ip_weight_window: int = 60    # Window size in seconds
        # Track (timestamp, weight) tuples per IP for sliding window calculation
        self._ip_weight_history: dict[str, list[tuple[float, int]]] = defaultdict(list)

        # Limit concurrent REST fallbacks per IP so startup/reconnect bursts do not
        # overwhelm a single ccxt instance and bypass its sequential rate limiter.
        _rest_limit = exchange_config.get('rest_ip_concurrency_limit')
        self._rest_ip_concurrency_limit: int | None = (
            max(1, int(_rest_limit)) if _rest_limit is not None else None
        )
        self._rest_ip_semaphores: dict[str, asyncio.Semaphore] = {}
        self._rest_ip_inflight: dict[str, int] = defaultdict(int)

        # Create dedicated CCXT exchange instances for each IP
        self._ws_exchanges: dict[str, ccxt.Exchange] = {}
        # Separate REST exchange instances per IP (created lazily in main thread)
        self._rest_exchanges: dict[str, ccxt.Exchange] = {}
        self._ip_stats: dict[str, dict] = {}

        # Diagnostic metrics per IP
        self._ip_metrics: dict[str, dict] = {}

        # Wallet address for rate limit queries (Hyperliquid-specific)
        # Use explicitly passed wallet_address, then fallback
        self._wallet_address: str = wallet_address or ''

        # Fallback to exchange_config if not passed directly
        if not self._wallet_address:
            self._wallet_address = (
                exchange_config.get('walletAddress', '')
                or exchange_config.get('wallet_address', '')
                or ''
            )

        # Fallback to ccxt_object attribute
        if not self._wallet_address and hasattr(ccxt_object, 'walletAddress'):
            self._wallet_address = ccxt_object.walletAddress or ''

        # Log wallet address status at startup
        if self._wallet_address:
            addr = self._wallet_address
            logger.info(
                f"[IP-POOL] Wallet address configured for "
                f"rate limit monitoring: {addr[:10]}...{addr[-6:]}"
            )
        else:
            logger.warning(
                "[IP-POOL] No wallet address found - rate limit "
                "monitoring will be disabled. Check "
                "'walletAddress' in exchange config."
            )

        if self._ip_pool:
            self._ws_exchanges = self._create_ws_exchange_pool(ccxt_object)
            # Initialize states: ALL IPs are active (pair distribution mode)
            for ip in self._ip_pool:
                self._ip_states[ip] = IPState.ACTIVE
                self._ip_session_times[ip] = time.time()
                self._ip_stats[ip] = {'active': 0, 'failures': 0, 'last_failure': None}
                self._ip_metrics[ip] = {
                    'subscriptions': 0,
                    'candles_received': 0,
                    'last_candle_ts': 0,
                    'errors': [],
                }
            logger.info(
                f"[IP-POOL] Initialized PAIR DISTRIBUTION mode with {len(self._ip_pool)} IPs. "
                f"All IPs active: {self._ip_pool}"
            )
        else:
            # No IP pool - use single exchange for everything
            self._ws_exchanges = {'default': ccxt_object}
            self._ip_stats['default'] = {'active': 0, 'failures': 0, 'last_failure': None}

        self._thread = Thread(name="ccxt_ws", target=self._start_forever)
        self._thread.start()
        self.__cleanup_called = False

        # Track last hourly log time
        self._last_hourly_log: float = 0

    @staticmethod
    def _patch_ccxt_sync_local_addr(exchange, ip: str) -> None:
        """
        Bind a ccxt sync exchange's requests.Session to a specific local IP.
        Uses a custom HTTPAdapter with urllib3's source_address support.
        """
        from requests.adapters import HTTPAdapter
        from urllib3 import PoolManager

        class SourceAddressAdapter(HTTPAdapter):
            def __init__(self, source_address, **kwargs):
                self._source_address = source_address
                super().__init__(**kwargs)

            def init_poolmanager(self, *args, **kwargs):
                kwargs['source_address'] = self._source_address
                super().init_poolmanager(*args, **kwargs)

        adapter = SourceAddressAdapter(source_address=(ip, 0))
        exchange.session.mount('https://', adapter)
        exchange.session.mount('http://', adapter)

    @staticmethod
    def _patch_ccxt_local_addr(exchange: ccxt.Exchange, ip: str) -> None:
        """
        Monkey-patch a ccxt async exchange instance to bind its TCP connections
        to a specific local IP address. This replaces the need for a custom ccxt
        fork — standard upstream ccxt does not support local_addr natively.

        Patches open() so that when ccxt creates its session (guarded by
        `self.session is None`), the TCPConnector includes local_addr.
        The patch only acts on the first call (when session is actually created)
        to avoid leaking connectors on subsequent no-op open() calls.
        """
        original_open = exchange.open

        def patched_open(*args, **kwargs):
            session_existed = exchange.session is not None
            result = original_open(*args, **kwargs)
            # Only patch the connector on the first open() when session is created
            if not session_existed and exchange.session is not None:
                old_connector = exchange.tcp_connector
                exchange.tcp_connector = aiohttp.TCPConnector(
                    ssl=exchange.ssl_context if hasattr(exchange, 'ssl_context') else None,
                    local_addr=(ip, 0),
                )
                exchange.session._connector = exchange.tcp_connector
                if old_connector is not None:
                    asyncio.ensure_future(old_connector.close())
            return result

        exchange.open = patched_open

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
            }
        }

        # Add optional credentials if present (for exchanges like Hyperliquid)
        if hasattr(template, 'walletAddress') and template.walletAddress:
            exchange_config['walletAddress'] = template.walletAddress
        if hasattr(template, 'privateKey') and template.privateKey:
            exchange_config['privateKey'] = template.privateKey

        instance = exchange_class(exchange_config)

        # Bind this instance's connections to the specified IP
        self._patch_ccxt_local_addr(instance, ip)

        return instance

    def _create_ws_exchange_pool(self, template: ccxt.Exchange) -> dict[str, ccxt.Exchange]:
        """Create dedicated CCXT exchange instances for each IP in the pool."""
        exchanges = {}
        for ip in self._ip_pool:
            exchanges[ip] = self._create_single_ws_exchange(ip)
            logger.debug(f"[IP-POOL] Created WebSocket exchange for IP {ip}")
        return exchanges

    def _get_ws_exchange_for_pair(self, pair: str) -> tuple[ccxt.Exchange, str]:
        """
        Get the WebSocket exchange for a pair using least-loaded distribution.
        New pairs are assigned to the IP with fewest current assignments,
        ensuring even distribution across all IPs.
        """
        if not self._ip_pool:
            return self._ws_exchanges.get('default', self._ccxt_object), 'default'

        # Check cache first
        if pair in self._pair_ip_assignment:
            assigned_ip = self._pair_ip_assignment[pair]
            # Verify IP is still active
            if self._ip_states.get(assigned_ip) == IPState.ACTIVE:
                return self._ws_exchanges[assigned_ip], assigned_ip
            # IP failed - need to reassign
            logger.debug(
                f"[PAIR-REASSIGN] Pair {pair} was on failed IP {assigned_ip}, "
                f"reassigning to active IP"
            )

        # Get list of active IPs for assignment
        active_ips = [ip for ip in self._ip_pool if self._ip_states.get(ip) == IPState.ACTIVE]

        if not active_ips:
            logger.error("[IP-POOL] No active IPs available! Falling back to default exchange.")
            return self._ws_exchanges.get('default', self._ccxt_object), 'default'

        # Count current assignments per active IP
        ip_counts = {ip: 0 for ip in active_ips}
        for assigned_pair, assigned_ip in self._pair_ip_assignment.items():
            if assigned_ip in ip_counts:
                ip_counts[assigned_ip] += 1

        # Assign to IP with fewest pairs (least-loaded distribution)
        assigned_ip = min(ip_counts, key=ip_counts.get)
        current_count = ip_counts[assigned_ip]

        # Log new assignment
        was_reassigned = pair in self._pair_ip_assignment
        old_ip = self._pair_ip_assignment.get(pair)

        # Cache the assignment
        self._pair_ip_assignment[pair] = assigned_ip

        # Log reassignments at INFO (important event), new assignments at DEBUG
        if was_reassigned:
            logger.debug(
                f"[PAIR-ASSIGN] {pair} reassigned: {old_ip} -> {assigned_ip} "
                f"(now has {current_count + 1} pairs)"
            )
        else:
            logger.debug(
                f"[PAIR-ASSIGN] {pair} -> {assigned_ip} "
                f"(least-loaded, now has {current_count + 1} pairs)"
            )

        return self._ws_exchanges[assigned_ip], assigned_ip

    def _count_pairs_per_ip(self) -> dict[str, int]:
        """Count pairs assigned to each IP."""
        counts: dict[str, int] = {ip: 0 for ip in self._ip_pool}
        for pair, ip in self._pair_ip_assignment.items():
            if ip in counts:
                counts[ip] += 1
        return counts

    def _log_ip_stats(self) -> None:
        """Log current IP pool statistics with pair distribution info.

        Called once per hour with the :20 refresh - provides essential visibility
        into IP distribution without log noise.
        """
        if not self._ip_pool:
            return
        pairs_per_ip = self._count_pairs_per_ip()
        stats_str = " | ".join(
            f"{ip}: pairs={pairs_per_ip.get(ip, 0)}, "
            f"streams={s['active']}, failures={s['failures']}"
            for ip, s in self._ip_stats.items()
        )
        logger.info(f"[IP-STATS] {stats_str}")

    def _log_distribution_state(self) -> None:
        """Log current pair distribution state for debugging (DEBUG level)."""
        if not self._ip_pool:
            return
        states = {ip: self._ip_states.get(ip, IPState.ACTIVE).value for ip in self._ip_pool}
        pairs_per_ip = self._count_pairs_per_ip()

        total_pairs = len(self._pair_ip_assignment)
        logger.debug(
            f"[DISTRIBUTION-STATE] Total pairs: {total_pairs} | "
            f"Per-IP: {pairs_per_ip} | States: {states}"
        )

    # =====================================================================
    # Public API for REST fallback IP routing
    # =====================================================================

    def get_ip_for_pair(self, pair: str) -> str | None:
        """Get the IP assigned to a pair for consistent routing.

        Used by REST fallback to route requests through the same IP as WebSocket.
        Returns None if pair not assigned or no IP pool configured.
        """
        if not self._ip_pool:
            return None
        return self._pair_ip_assignment.get(pair)

    def get_exchange_for_pair(self, pair: str) -> ccxt.Exchange | None:
        """Get the CCXT exchange instance bound to this pair's assigned IP.

        Reuses the same instances used for WebSocket - they're bound to an IP
        via local_addr and work for any CCXT call (WebSocket or REST).

        Returns None if pair not assigned or no IP pool configured.
        """
        ip = self._pair_ip_assignment.get(pair)
        if not ip:
            return None
        return self._ws_exchanges.get(ip)

    def get_rest_exchange_for_pair(self, pair: str) -> ccxt.Exchange | None:
        """Get REST-specific exchange for this pair's IP (separate from WebSocket).

        Creates instances lazily in main thread to bind to main event loop.
        Auto-assigns pair to an IP if not already assigned, so REST calls
        during startup (before WS subscribes) are also distributed.
        """
        if not self._ip_pool:
            return None

        ip = self._pair_ip_assignment.get(pair)
        if not ip:
            ip = self.assign_pair_to_ip(pair)
        if not ip:
            return None

        # Lazy creation ensures binding to main thread's event loop
        if ip not in self._rest_exchanges:
            logger.info(f"[REST-EXCHANGE] Creating REST instance for IP {ip}")
            self._rest_exchanges[ip] = self._create_single_ws_exchange(ip)

        return self._rest_exchanges.get(ip)

    def has_ip_pool(self) -> bool:
        """Return whether this exchange websocket helper is using an IP pool."""
        return bool(self._ip_pool)

    def _get_rest_ip_semaphore(self, ip: str) -> asyncio.Semaphore:
        """Get or create the per-IP REST concurrency limiter."""
        if self._rest_ip_concurrency_limit is None:
            raise RuntimeError("REST IP concurrency limiter is disabled.")
        if ip not in self._rest_ip_semaphores:
            self._rest_ip_semaphores[ip] = asyncio.Semaphore(self._rest_ip_concurrency_limit)
        return self._rest_ip_semaphores[ip]

    async def fetch_rest_ohlcv_for_pair(
        self,
        pair: str,
        timeframe: str,
        since_ms: int | None,
        candle_limit: int,
        params: dict,
        fallback_exchange: ccxt.Exchange | None = None,
    ) -> list:
        """Fetch REST OHLCV for a pair with per-IP concurrency limiting."""
        rest_api = self.get_rest_exchange_for_pair(pair)

        if rest_api is None:
            if fallback_exchange is None:
                raise RuntimeError("No REST exchange available for OHLCV fetch.")
            return await fallback_exchange.fetch_ohlcv(
                pair,
                timeframe=timeframe,
                since=since_ms,
                limit=candle_limit,
                params=params,
            )

        ip = self._pair_ip_assignment.get(pair)
        if not ip:
            return await rest_api.fetch_ohlcv(
                pair,
                timeframe=timeframe,
                since=since_ms,
                limit=candle_limit,
                params=params,
            )

        if self._rest_ip_concurrency_limit is None:
            return await rest_api.fetch_ohlcv(
                pair,
                timeframe=timeframe,
                since=since_ms,
                limit=candle_limit,
                params=params,
            )

        wait_started = time.monotonic()
        semaphore = self._get_rest_ip_semaphore(ip)

        async with semaphore:
            waited_for = time.monotonic() - wait_started
            self._rest_ip_inflight[ip] += 1
            current_inflight = self._rest_ip_inflight[ip]
            try:
                if waited_for >= 0.25:
                    logger.debug(
                        f"[REST-LIMITER] {pair}/{timeframe} waited {waited_for:.2f}s "
                        f"for IP {ip} slot (inflight={current_inflight}/"
                        f"{self._rest_ip_concurrency_limit})"
                    )
                return await rest_api.fetch_ohlcv(
                    pair,
                    timeframe=timeframe,
                    since=since_ms,
                    limit=candle_limit,
                    params=params,
                )
            finally:
                self._rest_ip_inflight[ip] -= 1

    def assign_pair_to_ip(self, pair: str) -> str | None:
        """Assign a pair to an IP if not already assigned.

        Uses least-loaded distribution strategy (same as WebSocket).
        Returns the assigned IP, or None if no IP pool configured.

        This should be called before REST candle fetching to ensure
        even distribution across IPs from startup.
        """
        if not self._ip_pool:
            return None

        # Already assigned - return existing
        if pair in self._pair_ip_assignment:
            return self._pair_ip_assignment[pair]

        # Use existing logic from _get_ws_exchange_for_pair
        _, assigned_ip = self._get_ws_exchange_for_pair(pair)
        return assigned_ip if assigned_ip != 'default' else None

    # =====================================================================

    def _log_metrics_summary(self) -> None:
        """Log comprehensive metrics for all IPs (DEBUG level)."""
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

            logger.debug(
                f"[METRICS] IP={ip} "
                f"state={getattr(self._ip_states.get(ip, 'unknown'), 'value', 'unknown')} "
                f"subscriptions={sub_count} "
                f"candles_received={m.get('candles_received', 0)} "
                f"last_candle={last_candle_str} "
                f"recent_errors={len(m.get('errors', []))}"
            )

    def _start_forever(self) -> None:
        self._loop = asyncio.new_event_loop()

        # Start stats monitor if IP pool is configured
        if self._ip_pool:
            self._loop.create_task(self._stats_monitor())
            logger.debug(f"[IP-POOL] Started stats monitor for {len(self._ip_pool)} IPs")

        try:
            self._loop.run_forever()
        finally:
            if self._loop.is_running():
                self._loop.stop()

    async def _stats_monitor(self) -> None:
        """
        Monitor pair distribution and handle:
        - Periodic connection refresh at :20 (40 min before candle boundary)
        - IP recovery after cooldown
        - Hourly stats logging (with refresh)

        Logging is minimal at INFO level - use -vv for detailed DEBUG output.
        """
        while True:
            await asyncio.sleep(30)  # Check every 30 seconds

            if not self._ip_pool:
                continue

            current_time = time.time()
            current_minute = int(current_time // 60) % 60

            # Periodic connection refresh at :20 to ensure fresh connections before :00
            # NOTE: Only refresh at :20, NOT :50. The :50 refresh is too close to the :00
            # candle boundary - connections take 25-60s to get first message, leaving
            # insufficient buffer time. The :20 refresh provides 40 minutes of margin.
            if current_minute == 20:
                # Only refresh if we haven't refreshed in the last 30 minutes
                if current_time - self._last_periodic_refresh > 1800:
                    await self._refresh_all_connections()
                    # Log IP stats once per hour with the refresh
                    self._log_ip_stats()

            # Try to recover failed IPs after cooldown
            await self._try_recover_failed_ips()

            # DEBUG-level logging for detailed diagnostics (visible with -vv)
            # Log distribution state every 5 minutes
            if int(current_time) % 300 < 30:  # Every 5 minutes (within 30s window)
                self._log_distribution_state()
                self._log_metrics_summary()
                self._log_ip_weight_status()
                await self._check_rate_limits_per_ip()

            # Extra DEBUG logging near candle boundaries (minute 58-02)
            if current_minute >= 58 or current_minute <= 2:
                self._log_candle_boundary_status(current_minute)

    def _log_comprehensive_ip_health(self) -> None:
        """Log comprehensive health status for all IPs (DEBUG level)."""
        if not self._ip_pool:
            return

        current_time = time.time()
        now_str = datetime.now().strftime('%H:%M:%S')

        for ip in self._ip_pool:
            ws_ex = self._ws_exchanges.get(ip)
            state = self._ip_states.get(ip, IPState.ACTIVE)
            stats = self._ip_stats.get(ip, {})

            # Count pairs assigned to this IP
            pairs_on_ip = [
                p for p, assigned_ip
                in self._pair_ip_assignment.items()
                if assigned_ip == ip
            ]
            num_pairs = len(pairs_on_ip)

            # Count actual ohlcv subscriptions
            sub_count = 0
            freshness_info = []
            if ws_ex and hasattr(ws_ex, 'ohlcvs'):
                for pair, tf_data in ws_ex.ohlcvs.items():
                    for tf, candles in tf_data.items():
                        sub_count += 1
                        if candles:
                            last_ts = candles[-1][0]
                            age_sec = (current_time * 1000 - last_ts) / 1000
                            freshness_info.append((pair, tf, age_sec))

            # Calculate average data age (only for 1h candles to avoid 4h skewing the average)
            freshness_1h = [f for f in freshness_info if f[1] == '1h']
            avg_age = sum(f[2] for f in freshness_1h) / len(freshness_1h) if freshness_1h else -1

            # Count stale streams - threshold depends on timeframe
            def is_stale(pair: str, tf: str, age_sec: float) -> bool:
                if tf == '4h':
                    return age_sec > 14700  # 4h + 5min = 245 min
                else:
                    return age_sec > 3900   # 1h + 5min = 65 min

            stale_count = sum(1 for p, tf, age in freshness_info if is_stale(p, tf, age))

            # Connection uptime
            uptime_sec = current_time - self._ip_connection_start.get(ip, current_time)

            logger.debug(
                f"[IP-HEALTH] {now_str} IP={ip} state={state.value} "
                f"pairs_assigned={num_pairs} streams={sub_count} "
                f"avg_data_age={avg_age:.1f}s stale_streams={stale_count} "
                f"failures={stats.get('failures', 0)} uptime={uptime_sec:.0f}s"
            )

    def _log_candle_boundary_status(self, current_minute: int) -> None:
        """Log detailed status near candle boundaries for debugging (debug level)."""
        if not self._ip_pool:
            return

        current_time = time.time()
        now_str = datetime.now().strftime('%H:%M:%S.%f')[:-3]

        logger.debug(
            f"[CANDLE-BOUNDARY] ========== "
            f"Status at :{current_minute:02d} ({now_str}) "
            f"=========="
        )

        for ip in self._ip_pool:
            ws_ex = self._ws_exchanges.get(ip)
            if not ws_ex:
                continue

            # Sample first 3 pairs on this IP (reduced from 5)
            pairs_on_ip = [
                p for p, assigned_ip
                in self._pair_ip_assignment.items()
                if assigned_ip == ip
            ][:3]

            for pair in pairs_on_ip:
                for tf in ['1h', '4h']:
                    data = ws_ex.ohlcvs.get(pair, {}).get(tf, [])
                    if data:
                        last_ts = data[-1][0]
                        last_time_str = datetime.fromtimestamp(last_ts / 1000).strftime('%H:%M:%S')
                        age_sec = (current_time * 1000 - last_ts) / 1000
                        logger.debug(
                            f"[CANDLE-BOUNDARY] IP={ip} {pair}/{tf} "
                            f"candles={len(data)} last={last_time_str} age={age_sec:.1f}s"
                        )

    def _log_all_pair_assignments(self) -> None:
        """Log all pair-to-IP assignments for debugging (DEBUG level)."""
        if not self._ip_pool:
            return

        # Group pairs by IP
        pairs_by_ip: dict[str, list[str]] = {ip: [] for ip in self._ip_pool}
        for pair, ip in self._pair_ip_assignment.items():
            if ip in pairs_by_ip:
                pairs_by_ip[ip].append(pair)

        logger.debug(f"[PAIR-DISTRIBUTION] Total pairs assigned: {len(self._pair_ip_assignment)}")
        for ip in self._ip_pool:
            pairs = pairs_by_ip.get(ip, [])
            logger.debug(
                f"[PAIR-DISTRIBUTION] IP={ip} has {len(pairs)} pairs: "
                f"{', '.join(sorted(pairs)[:10])}{'...' if len(pairs) > 10 else ''}"
            )

    def _record_ip_weight(self, ip: str, weight: int) -> None:
        """Record a request weight for an IP and prune old entries."""
        now = time.time()
        cutoff = now - self._ip_weight_window

        # Add new entry
        self._ip_weight_history[ip].append((now, weight))

        # Prune entries older than window
        self._ip_weight_history[ip] = [
            (ts, w) for ts, w in self._ip_weight_history[ip]
            if ts > cutoff
        ]

    def _get_ip_weight_usage(self, ip: str) -> tuple[int, float]:
        """Get current weight usage for an IP within the sliding window.

        Returns:
            (current_weight, percentage_used)
        """
        now = time.time()
        cutoff = now - self._ip_weight_window

        # Sum weights in current window
        current_weight = sum(
            w for ts, w in self._ip_weight_history.get(ip, [])
            if ts > cutoff
        )

        pct = (current_weight / self._ip_weight_budget) * 100
        return current_weight, pct

    def _calculate_request_weight(self, request_type: str, response_items: int = 0) -> int:
        """Calculate weight for a request type based on Hyperliquid docs.

        Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits
        """
        # Fixed-weight endpoints
        fixed_weights = {
            'l2Book': 2, 'allMids': 2, 'clearinghouseState': 2,
            'orderStatus': 2, 'spotClearinghouseState': 2,
            'exchangeStatus': 2, 'userRole': 60, 'explorer': 40,
            'userRateLimit': 2,  # Our rate limit check
        }

        if request_type in fixed_weights:
            return fixed_weights[request_type]

        # Variable-weight endpoints (base + items/divisor)
        variable_weights = {
            'candleSnapshot': (20, 60),
            'recentTrades': (20, 20),
            'historicalOrders': (20, 20),
            'userFills': (20, 20),
            'userFillsByTime': (20, 20),
            'fundingHistory': (20, 20),
            'userFunding': (20, 20),
            'twapHistory': (20, 20),
        }

        if request_type in variable_weights:
            base, divisor = variable_weights[request_type]
            return base + (response_items // divisor)

        # Default for other info requests
        return 20

    def _log_ip_weight_status(self) -> None:
        """Log current rate limit weight usage for all IPs and REST proxy (DEBUG level).

        Warnings for high usage (>70%) remain at WARNING level.
        """
        # Log REST proxy consumption (all REST calls go through single proxy)
        rest_usage, rest_pct = self._get_ip_weight_usage("REST_PROXY")
        if rest_usage > 0:
            logger.debug(
                f"[REST-WEIGHT] REST_PROXY="
                f"{rest_usage}/{self._ip_weight_budget}"
                f"({rest_pct:.0f}%)"
            )
            if rest_pct > 70:
                logger.warning(
                    f"[REST-WEIGHT-HIGH] REST proxy at "
                    f"{rest_pct:.0f}% of rate limit budget"
                )

        # Log WebSocket IP consumption (direct connections)
        if not self._ip_pool:
            return

        weight_status = []
        for ip in self._ip_pool:
            usage, pct = self._get_ip_weight_usage(ip)
            weight_status.append(f"{ip}={usage}/{self._ip_weight_budget}({pct:.0f}%)")
            if pct > 70:
                logger.warning(f"[IP-WEIGHT-HIGH] IP={ip} at {pct:.0f}% of rate limit budget")

        logger.debug(f"[IP-WEIGHT] {' | '.join(weight_status)}")

    async def _check_rate_limits_per_ip(self) -> None:
        """
        Query Hyperliquid API for rate limit consumption from EACH IP.
        This helps diagnose which IPs are approaching rate limits.
        Runs all IP checks in parallel using asyncio.gather().
        """
        if not self._ip_pool:
            return

        # Only run for Hyperliquid - other exchanges have different rate limit APIs
        exchange_name = self.config.get('exchange', {}).get('name', '').lower()
        if exchange_name != 'hyperliquid':
            return

        if not self._wallet_address:
            logger.warning("[RATE-LIMIT] No wallet address configured, skipping rate limit check")
            return

        async def check_single_ip(ip: str) -> None:
            """Check rate limit for a single IP."""
            try:
                # Create connector bound to this specific IP
                connector = aiohttp.TCPConnector(local_addr=(ip, 0))
                timeout = aiohttp.ClientTimeout(total=10)

                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.post(
                        "https://api.hyperliquid.xyz/info",
                        json={"type": "userRateLimit", "user": self._wallet_address},
                        headers={"Content-Type": "application/json"}
                    ) as resp:
                        # Track this request's weight (userRateLimit = 2 weight)
                        self._record_ip_weight(ip, self._calculate_request_weight('userRateLimit'))

                        if resp.status == 200:
                            data = await resp.json()

                            n_requests_used = data.get('nRequestsUsed', 0)
                            n_requests_cap = data.get('nRequestsCap', 0)
                            n_requests_surplus = data.get('nRequestsSurplus', 0)
                            cum_vlm = data.get('cumVlm', '0')

                            # Calculate usage percentage
                            usage_pct = (n_requests_used / max(n_requests_cap, 1)) * 100

                            # Store in metrics
                            if ip in self._ip_metrics:
                                self._ip_metrics[ip]['rate_limit'] = {
                                    'nRequestsUsed': n_requests_used,
                                    'nRequestsCap': n_requests_cap,
                                    'nRequestsSurplus': n_requests_surplus,
                                    'cumVlm': cum_vlm,
                                    'usage_pct': usage_pct,
                                    'checked_at': datetime.now().strftime('%H:%M:%S')
                                }

                            logger.debug(
                                f"[RATE-LIMIT] IP={ip} "
                                f"used={n_requests_used}/{n_requests_cap} "
                                f"({usage_pct:.1f}%) surplus={n_requests_surplus}"
                            )

                            # Warn if approaching limit (keep at WARNING)
                            if usage_pct > 80:
                                logger.warning(
                                    f"[RATE-LIMIT] IP={ip} approaching limit ({usage_pct:.1f}%)"
                                )
                        else:
                            logger.warning(
                                f"[RATE-LIMIT] IP={ip} rate limit query failed: HTTP {resp.status}"
                            )

            except TimeoutError:
                logger.warning(f"[RATE-LIMIT] IP={ip} rate limit query timed out")
            except Exception as e:
                logger.warning(f"[RATE-LIMIT] IP={ip} rate limit query error: {e}")

        # Get active IPs and run all checks in parallel
        active_ips = [ip for ip in self._ip_pool if self._ip_states.get(ip) == IPState.ACTIVE]
        if active_ips:
            logger.debug(f"[RATE-LIMIT] Checking rate limits for {len(active_ips)} IPs...")
            await asyncio.gather(
                *[check_single_ip(ip) for ip in active_ips],
                return_exceptions=True,
            )

    async def _refresh_all_connections(self) -> None:
        """
        Refresh all WebSocket connections to ensure freshness.
        Called at :20 to ensure fresh connections before candle close at :00.
        The 40-minute buffer allows connections time to stabilize (first message
        can take 25-60 seconds to arrive).
        """
        if not self._ip_pool:
            return

        current_minute = int(time.time() // 60) % 60
        logger.info(f"[WS-REFRESH] Starting periodic refresh at :{current_minute:02d}")

        # Close all existing connections
        for ip, ws_exchange in self._ws_exchanges.items():
            try:
                await ws_exchange.close()
                ws_exchange.ohlcvs.clear()
                logger.debug(f"[WS-REFRESH] Closed connection for IP {ip}")
            except Exception as e:
                logger.warning(f"[WS-REFRESH] Error closing {ip}: {e}")

        # Recreate exchange instances
        self._ws_exchanges = self._create_ws_exchange_pool(self._ccxt_object)

        # Clear pair assignments - will be re-assigned on next request
        self._pair_ip_assignment.clear()

        # Reset connection start times
        self._ip_connection_start.clear()

        # Clear scheduled pairs so they get rescheduled
        self._klines_scheduled.clear()

        # Reset IP states to active and clear failure tracking
        for ip in self._ip_pool:
            self._ip_states[ip] = IPState.ACTIVE
            self._ip_session_times[ip] = time.time()
            self._ip_consecutive_failures[ip] = 0
            self._ip_backoff_delay[ip] = 0  # Reset backoff on refresh
            self._ip_failure_time.pop(ip, None)

        # Update last refresh time
        self._last_periodic_refresh = time.time()

        # Re-schedule all pairs that were being watched
        # This ensures WS subscriptions are re-established immediately after refresh
        watched_pairs = list(self._klines_watching)
        if watched_pairs:
            logger.debug(
                f"[WS-REFRESH] Re-scheduling {len(watched_pairs)} watched pairs after refresh"
            )
            # Trigger scheduling for all watched pairs (already in async context)
            await self._schedule_while_true()

        logger.info(f"[WS-REFRESH] Completed - {len(self._ip_pool)} IPs refreshed")

    async def _try_recover_failed_ips(self) -> None:
        """
        Try to recover failed IPs after 5 minute cooldown.
        This gives the IP a chance to recover from transient issues.
        After recovery, re-schedules pairs that were on this IP.
        """
        if not self._ip_pool:
            return

        current_time = time.time()

        for ip in self._ip_pool:
            if self._ip_states.get(ip) == IPState.FAILED:
                failure_time = self._ip_failure_time.get(ip, 0)
                time_since_failure = current_time - failure_time

                if time_since_failure > self._recovery_cooldown:
                    logger.debug(
                        f"[IP-RECOVERY] Attempting to recover IP {ip} "
                        f"(failed {time_since_failure:.0f}s ago)"
                    )

                    try:
                        # Recreate the exchange instance for this IP
                        if ip in self._ws_exchanges:
                            try:
                                await self._ws_exchanges[ip].close()
                            except Exception:  # noqa: S110
                                pass

                        self._ws_exchanges[ip] = self._create_single_ws_exchange(ip)
                        self._ip_states[ip] = IPState.ACTIVE
                        self._ip_consecutive_failures[ip] = 0
                        self._ip_backoff_delay[ip] = 0  # Reset backoff on recovery
                        self._ip_session_times[ip] = current_time
                        self._ip_failure_time.pop(ip, None)

                        logger.info(f"[IP-RECOVERY] IP {ip} recovered")

                        # Re-schedule pairs that need an IP assignment
                        # After IP failure, pair assignments are cleared, so look for
                        # desired subscriptions that are NOT currently scheduled
                        pairs_to_reschedule = [
                            p for p in self._desired_subscriptions
                            if p not in self._klines_scheduled
                        ]

                        if pairs_to_reschedule:
                            for p in pairs_to_reschedule:
                                # Allow re-scheduling by removing from scheduled set
                                self._klines_scheduled.discard(p)

                            logger.debug(
                                f"[IP-RECOVERY] Re-scheduling {len(pairs_to_reschedule)} pairs "
                                f"after IP {ip} recovery"
                            )

                            # Trigger rescheduling
                            await self._schedule_while_true()

                    except Exception as e:
                        logger.warning(f"[IP-RECOVERY] Failed to recover IP {ip}: {e}")

    async def _handle_ip_failure(self, failed_ip: str, pair: str) -> None:
        """
        Handle failure of an IP in pair distribution mode.
        Uses threshold (3 consecutive failures) before marking as FAILED.
        Failed IPs can recover after 5 minute cooldown.
        """
        # Increment consecutive failure count
        self._ip_consecutive_failures[failed_ip] = (
            self._ip_consecutive_failures.get(failed_ip, 0) + 1
        )
        failure_count = self._ip_consecutive_failures[failed_ip]

        # Update stats
        self._ip_stats[failed_ip]['failures'] += 1
        self._ip_stats[failed_ip]['last_failure'] = (
            f"{pair} at {datetime.now().strftime('%H:%M:%S')}"
        )

        # Only mark as FAILED after configured threshold of consecutive failures
        if failure_count < self._failure_threshold:
            logger.warning(
                f"[IP-FAILURE] IP {failed_ip} failed on {pair} "
                f"(failure {failure_count}/{self._failure_threshold}, not yet marking as FAILED)"
            )
            return

        # Already marked as failed
        if self._ip_states.get(failed_ip) == IPState.FAILED:
            return

        # Mark as FAILED and record time for recovery cooldown
        self._ip_states[failed_ip] = IPState.FAILED
        self._ip_failure_time[failed_ip] = time.time()

        logger.warning(
            f"[IP-FAILURE] IP {failed_ip} marked FAILED after {failure_count} consecutive failures "
            f"(last pair: {pair})"
        )

        # Clear pair assignments for this IP so they get reassigned to active IPs
        pairs_to_reassign = [
            p for p, ip in self._pair_ip_assignment.items()
            if ip == failed_ip
        ]
        for p in pairs_to_reassign:
            del self._pair_ip_assignment[p]

        active_remaining = len([
            ip for ip in self._ip_pool
            if self._ip_states.get(ip) == IPState.ACTIVE
        ])
        logger.info(
            f"[IP-FAILURE] Cleared {len(pairs_to_reassign)} pairs from {failed_ip}, "
            f"{active_remaining} IPs remaining active"
        )

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

    def _get_active_ips(self) -> list[str]:
        """Return list of active IPs in the pool."""
        return [ip for ip in self._ip_pool if self._ip_states.get(ip) == IPState.ACTIVE]

    @retrier(retries=3)
    def ohlcvs(self, pair: str, timeframe: str) -> list[list]:
        """
        Returns a copy of the klines for a pair/timeframe combination.
        In pair distribution mode, gets data from the assigned IP for this pair.
        """
        try:
            # Get the exchange assigned to this pair
            ws_exchange, assigned_ip = self._get_ws_exchange_for_pair(pair)

            data = ws_exchange.ohlcvs.get(pair, {}).get(timeframe, [])

            if data:
                # Debug-level logging only - reduces log volume significantly
                logger.debug(
                    f"[OHLCV-READ] {pair}/{timeframe} from IP {assigned_ip}, "
                    f"candles={len(data)}"
                )
                # Snapshot to plain list first to avoid "deque mutated during
                # iteration" when the WS thread appends/pops concurrently.
                # See docs/freqtrade-fork-customizations.md for details.
                return [list(c) for c in list(data)]

            # No data from assigned IP - shouldn't happen normally
            ip_state = getattr(
                self._ip_states.get(assigned_ip, 'unknown'),
                'value', 'unknown',
            )
            logger.warning(
                f"[OHLCV-MISSING] No data for "
                f"{pair}/{timeframe} from assigned IP "
                f"{assigned_ip} (state={ip_state})"
            )

            # Fallback to default/main exchange (no IP pool case)
            default_exchange = self._ws_exchanges.get('default', self._ccxt_object)
            fallback = default_exchange.ohlcvs.get(pair, {}).get(timeframe, [])
            return [list(c) for c in list(fallback)]
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
                # NOTE: Do NOT remove from _desired_subscriptions here.
                # Stale entries are harmless (won't be scheduled since not in _klines_watching)
                # and removing could break recovery if schedule_ohlcv isn't called frequently.
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

                logger.debug(
                    f"[WS-SCHEDULE] {pair}/{timeframe} -> IP {assigned_ip} "
                    f"(active on IP: {self._ip_stats.get(assigned_ip, {}).get('active', '?')})"
                )

                task = asyncio.create_task(
                    self._continuously_async_watch_ohlcv(
                        pair, timeframe, candle_type,
                        ws_exchange, assigned_ip,
                    )
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
            current = self._ip_stats[assigned_ip]['active']
            self._ip_stats[assigned_ip]['active'] = max(
                0, current - 1
            )

        result = "done"
        if task.cancelled():
            result = "cancelled"
        else:
            if (result1 := task.result()) is not None:
                result = str(result1)

        logger.debug(
            f"[WS-TASK-DONE] {pair}/{timeframe} on IP {assigned_ip} - result: {result} "
            f"(remaining active on IP: {self._ip_stats.get(assigned_ip, {}).get('active', '?')})"
        )
        asyncio.run_coroutine_threadsafe(
            self._unwatch_ohlcv(pair, timeframe, candle_type), loop=self._loop
        )

        self._klines_scheduled.discard((pair, timeframe, candle_type))
        self._pop_history((pair, timeframe, candle_type))

    async def _continuously_async_watch_ohlcv(  # noqa: C901
        self, pair: str, timeframe: str, candle_type: CandleType,
        ws_exchange: ccxt.Exchange, assigned_ip: str
    ) -> None:
        first_message_received = False
        message_count = 0
        connect_start = time.time()  # For TTFM tracking

        # Apply exponential backoff if there's a pending delay for this IP
        backoff = self._ip_backoff_delay.get(assigned_ip, 0)
        if backoff > 0:
            logger.debug(
                f"[WS-BACKOFF] Applying {backoff:.1f}s backoff delay for IP {assigned_ip} "
                f"before connecting {pair}/{timeframe}"
            )
            await asyncio.sleep(backoff)

        try:
            logger.debug(
                f"[WS-CONNECT-START] Opening WebSocket for {pair}/{timeframe} on IP {assigned_ip} "
                f"at {datetime.now().strftime('%H:%M:%S')}"
            )
            while (pair, timeframe, candle_type) in self._klines_watching:
                start = dt_ts()
                data = await ws_exchange.watch_ohlcv(pair, timeframe)
                self.klines_last_refresh[(pair, timeframe, candle_type)] = dt_ts()
                message_count += 1

                # Track metrics
                if assigned_ip in self._ip_metrics:
                    self._ip_metrics[assigned_ip]['candles_received'] += 1
                    if data:
                        self._ip_metrics[assigned_ip]['last_candle_ts'] = data[-1][0]

                if not first_message_received:
                    first_message_received = True

                    # Calculate and log Time-to-First-Message (TTFM)
                    ttfm = time.time() - connect_start
                    logger.debug(
                        f"[WS-TTFM] {pair}/{timeframe} "
                        f"IP={assigned_ip} "
                        f"time_to_first_message={ttfm:.2f}s"
                    )

                    # Track connection time for this IP
                    if (assigned_ip not in self._ip_connection_start
                            and assigned_ip in self._ip_pool):
                        self._ip_connection_start[assigned_ip] = time.time()

                    # Reset backoff on successful connection
                    if (assigned_ip in self._ip_backoff_delay
                            and self._ip_backoff_delay[assigned_ip] > 0):
                        logger.debug(
                            f"[WS-BACKOFF] IP={assigned_ip} "
                            f"backoff reset after successful "
                            f"connection"
                        )
                        self._ip_backoff_delay[assigned_ip] = 0

                    # Reset consecutive failures on success
                    if assigned_ip in self._ip_consecutive_failures:
                        self._ip_consecutive_failures[assigned_ip] = 0

                    last_ts = data[-1][0] if data else 0
                    last_time_str = (
                        datetime.fromtimestamp(
                            last_ts / 1000
                        ).strftime('%H:%M:%S')
                        if last_ts else 'N/A'
                    )
                    logger.debug(
                        f"[WS-CONNECTED] First data for {pair}/{timeframe} on IP {assigned_ip} "
                        f"candles={len(data)} last_candle={last_time_str}"
                    )

                # Log periodic updates and near candle boundaries
                current_minute = int(time.time() // 60) % 60
                near_boundary = current_minute >= 58 or current_minute <= 2

                if near_boundary and data:
                    last_ts = data[-1][0]
                    last_time_str = datetime.fromtimestamp(last_ts / 1000).strftime('%H:%M:%S')
                    age_sec = (time.time() * 1000 - last_ts) / 1000
                    logger.debug(
                        f"[WS-UPDATE] :{current_minute:02d} {pair}/{timeframe} IP={assigned_ip} "
                        f"candles={len(data)} last={last_time_str} "
                        f"age={age_sec:.1f}s msg#={message_count}"
                    )

                logger.debug(
                    f"watch done {pair}, {timeframe}, IP {assigned_ip}, data {len(data)} "
                    f"in {(dt_ts() - start) / 1000:.3f}s"
                )
        except ccxt.ExchangeClosedByUser:
            logger.debug(
                f"[WS-CLOSED] Exchange closed by user "
                f"for {pair}/{timeframe} on IP {assigned_ip}"
            )
        except ccxt.BaseError as e:
            # DIAGNOSTIC: Log connection errors with full context
            error_time = datetime.now().strftime('%H:%M:%S.%f')
            current_minute = int(time.time() // 60) % 60

            # Track failure statistics
            if assigned_ip in self._ip_stats:
                self._ip_stats[assigned_ip]['failures'] += 1
                self._ip_stats[assigned_ip]['last_failure'] = str(e)[:100]

            # Increase exponential backoff: 1s -> 2s -> 4s -> 8s -> max (configurable, default 30s)
            current_backoff = self._ip_backoff_delay.get(assigned_ip, 0.5)
            new_backoff = min(current_backoff * 2, self._backoff_max)
            self._ip_backoff_delay[assigned_ip] = new_backoff
            failure_count = self._ip_consecutive_failures.get(assigned_ip, 0) + 1

            logger.error(
                f"[WS-CONN-ERROR] :{current_minute:02d} IP={assigned_ip} {pair}/{timeframe} "
                f"error={type(e).__name__}: {str(e)[:200]} "
                f"state={getattr(self._ip_states.get(assigned_ip, 'unknown'), 'value', 'unknown')} "
                f"at={error_time}"
            )

            logger.debug(
                f"[WS-BACKOFF] IP={assigned_ip} "
                f"backoff_delay={new_backoff:.1f}s "
                f"after {failure_count} failures"
            )

            # Track in metrics
            if assigned_ip in self._ip_metrics:
                self._ip_metrics[assigned_ip]['errors'].append({
                    'time': error_time,
                    'minute': current_minute,
                    'pair': pair,
                    'error': str(e)[:100]
                })
                ip_errors = self._ip_metrics[assigned_ip]['errors']
                self._ip_metrics[assigned_ip]['errors'] = ip_errors[-10:]

            # Handle IP failure - mark IP as failed and allow pair redistribution
            if self._ip_pool:
                task = asyncio.create_task(
                    self._handle_ip_failure(assigned_ip, pair)
                )
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
        finally:
            logger.debug(
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
        # Track in desired subscriptions for auto-resubscribe after recovery
        self._desired_subscriptions.add(paircomb)
        self.klines_last_request[paircomb] = dt_ts()
        asyncio.run_coroutine_threadsafe(self._schedule_while_true(), loop=self._loop)
        # NOTE: cleanup_expired() removed - periodic refresh at :20 handles
        # connection lifecycle. Pairs are naturally managed by pairlist updates.

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
