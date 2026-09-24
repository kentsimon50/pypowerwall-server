"""
TimeSeriesStore - Lightweight SQLite-backed time-series storage.

Persists raw 5-second power samples from the existing poll loop and derives
daily energy totals (kWh) per gateway via trapezoidal integration. Survives
restarts, adds no network calls, and can be disabled entirely for headless
proxy deployments (PW_TIMESERIES_RETENTION=-1).

Architecture:
    Two-layer storage keeps the on-disk footprint tiny by default:

    1. Raw samples (table ``samples``) - one row per gateway per poll cycle
       (~17k rows/day at the default 5s interval). Power readings are split
       into directional components at record time (battery charge/discharge,
       grid import/export) so no information is netted away. Pruned to the
       PW_TIMESERIES_RETENTION window by a background maintenance task.

    2. Daily aggregates (table ``daily_energy``) - one row per gateway per
       local day holding cumulative kWh per category. This is the layer that
       outlives raw-sample pruning: a full year of daily totals is ~1 KB per
       gateway. Retention governed by PW_TIMESERIES_DAILY_RETENTION.

    A third table (``integration_state``) stores the last integrated sample
    per gateway so energy accumulation resumes exactly where it left off
    after a restart, without double counting.

Energy integration:
    Trapezoidal integration between consecutive samples:
        kWh = (P0 + P1) / 2 * dt / 3_600_000   (P in watts, dt in seconds)

    - Intervals longer than the 1-hour gap threshold are NOT integrated
      (stale/outage data must not fabricate energy).
    - Intervals crossing local midnight (in the gateway's configured
      timezone) are split at the boundary so each day accrues only its own
      energy. DST transitions are handled naturally because integration is
      performed on real elapsed time; only day attribution uses local dates.

Thread safety:
    All SQLite access is serialized through a single worker thread
    (dedicated ThreadPoolExecutor(max_workers=1)) plus an RLock, mirroring
    the StatsTracker pattern. WAL mode allows the API readers to query
    without blocking the writer.

Environment Variables:
    PW_TIMESERIES_RETENTION       Raw sample retention (default "24h").
                                  Duration suffixes: s/m/h/d/w.
                                  "0" = unlimited, "-1" = disable subsystem
                                  entirely (no SQLite file, no UI panel).
    PW_TIMESERIES_DAILY_RETENTION Daily aggregate retention (default "0" =
                                  unlimited). One row/day/gateway is tiny,
                                  so unlimited is a sensible default.
    PW_TIMESERIES_PATH            SQLite file path (default "/data/timeseries.db"
                                  when /data exists — e.g. the Docker image —
                                  otherwise "data/timeseries.db" relative to
                                  the working directory). If set to a
                                  directory instead of a file (trailing "/"
                                  or an existing directory), "timeseries.db"
                                  is appended automatically.
"""

import asyncio
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)

# Categories tracked per sample. Sign conventions match PowerwallData:
# battery positive = discharging, site positive = importing.
CATEGORIES = (
    "solar",
    "home",
    "battery_charge",
    "battery_discharge",
    "grid_import",
    "grid_export",
)

# Do not integrate across gaps longer than this (seconds). A gateway that
# was unreachable for an hour must not come back and fabricate the energy
# it presumably did (or did not) move while gone.
GAP_THRESHOLD = 3600.0

# Raw samples are always kept for at least this long even when retention is
# configured shorter, preserving a troubleshooting window (seconds).
RAW_KEEP_FLOOR = 3600.0

# Maintenance (pruning) cadence in seconds.
MAINTENANCE_INTERVAL = 60.0

# Minimum seconds between repeated write-failure warnings.
_FAILURE_WARN_INTERVAL = 300.0

# UTC fallback zoneinfo object for gateways with unresolvable timezones.
_UTC = ZoneInfo("UTC")

# zoneinfo cache: timezone name -> ZoneInfo (or None when unresolvable)
_zone_cache: Dict[str, Optional[ZoneInfo]] = {}


def parse_duration(value: str) -> int:
    """Parse a retention setting into seconds.

    Accepted forms:
        "-1"          -> -1  (disabled)
        "0"           -> 0   (unlimited)
        "24h", "7d"   -> suffixed durations (s, m, h, d, w)
        "3600"        -> bare number treated as seconds

    Raises:
        ValueError: on unparseable input.
    """
    text = str(value).strip().lower()
    if not text:
        raise ValueError(f"invalid duration: {value!r}")
    if text == "-1":
        return -1
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    negative = text.startswith("-")
    if negative:
        text = text[1:]
    if text and (text[-1] in multipliers or text.isdigit()):
        if text[-1] in multipliers:
            number = text[:-1]
            suffix = text[-1]
        else:
            number = text
            suffix = "s"
        try:
            parsed = int(number)
        except ValueError:
            raise ValueError(f"invalid duration: {value!r}") from None
        if parsed < 0 or negative:
            return -1
        return parsed * multipliers[suffix]
    raise ValueError(f"invalid duration: {value!r}")


def _get_zone(name: Optional[str]) -> ZoneInfo:
    """Resolve a timezone name to ZoneInfo, cached, falling back to UTC."""
    if not name:
        return _UTC
    if name not in _zone_cache:
        try:
            _zone_cache[name] = ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError, OSError):
            logger.warning(
                "Unknown timezone %r for time-series aggregation - using UTC", name
            )
            _zone_cache[name] = None
    return _zone_cache[name] or _UTC


def _local_date(ts: float, zone: ZoneInfo):
    """Local calendar date for a unix timestamp."""
    return datetime.fromtimestamp(ts, zone).date()


def _midnights_between(t0: float, t1: float, zone: ZoneInfo) -> List[float]:
    """Timestamps of local midnights strictly inside the interval (t0, t1)."""
    out: List[float] = []
    cursor = datetime.fromtimestamp(t0, zone).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)
    while True:
        midnight = cursor.timestamp()
        if midnight >= t1:
            break
        if midnight > t0:
            out.append(midnight)
        cursor += timedelta(days=1)
    return out


class TimeSeriesStore:
    """Thread-safe SQLite time-series store for daily energy statistics."""

    _DEFAULT_FILENAME = "timeseries.db"

    def __init__(
        self,
        db_path: str,
        retention: Any = "24h",
        daily_retention: Any = "0",
    ):
        """Create the store.

        Args:
            db_path:         SQLite database file path. If this points at a
                             directory (trailing slash, or an existing
                             directory on disk) rather than a file, the
                             default filename ("timeseries.db") is appended
                             automatically — a common misconfiguration when
                             PW_TIMESERIES_PATH is set to a mounted volume.
            retention:       Raw sample retention (duration string or seconds).
                             -1 disables the store, 0 means unlimited.
            daily_retention: Daily aggregate retention (duration string or
                             seconds). 0 means unlimited.
        """
        self._db_path = self._resolve_db_path(str(db_path))
        self._retention = self._coerce(retention, "24h", "PW_TIMESERIES_RETENTION")
        self._daily_retention = self._coerce(
            daily_retention, "0", "PW_TIMESERIES_DAILY_RETENTION"
        )
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._maintenance_task: Optional[asyncio.Task] = None
        # Write-failure tracking: surfaced in status(), warned at most once
        # per _FAILURE_WARN_INTERVAL so a full disk is visible without spam.
        self._write_failures = 0
        self._last_failure_warn = 0.0
        # In-memory cache of the last integrated sample per gateway:
        # {gateway_id: {"ts": float, "values": {category: watts}}}
        self._state: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @classmethod
    def _resolve_db_path(cls, db_path: str) -> str:
        """Append the default filename when db_path looks like a directory.

        Guards against pointing PW_TIMESERIES_PATH at a directory (e.g. a
        mounted volume such as "/data") instead of a file — SQLite cannot
        open a directory as a database. The ":memory:" sentinel is passed
        through untouched.
        """
        if db_path == ":memory:":
            return db_path
        if db_path.endswith(("/", os.sep)) or os.path.isdir(db_path):
            return os.path.join(db_path, cls._DEFAULT_FILENAME)
        return db_path

    @staticmethod
    def _coerce(value: Any, default: str, setting: str) -> int:
        """Parse a retention value with a safe default on bad input."""
        if isinstance(value, bool):
            return parse_duration(default)
        if isinstance(value, (int, float)):
            result = int(value)
            return -1 if result < 0 else result
        try:
            return parse_duration(str(value))
        except ValueError:
            logger.warning(
                "Invalid %s value %r - using default %r", setting, value, default
            )
            return parse_duration(default)

    @property
    def enabled(self) -> bool:
        """True when the subsystem is active (retention != -1)."""
        return self._retention != -1

    def _ensure_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="timeseries"
            )
        return self._executor

    def _ensure_conn(self) -> sqlite3.Connection:
        """Open (lazily) and return the SQLite connection. Caller holds the lock."""
        if self._conn is None:
            directory = os.path.dirname(self._db_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS samples (
                    gateway_id TEXT NOT NULL,
                    ts REAL NOT NULL,
                    solar_w REAL NOT NULL DEFAULT 0,
                    home_w REAL NOT NULL DEFAULT 0,
                    battery_charge_w REAL NOT NULL DEFAULT 0,
                    battery_discharge_w REAL NOT NULL DEFAULT 0,
                    grid_import_w REAL NOT NULL DEFAULT 0,
                    grid_export_w REAL NOT NULL DEFAULT 0,
                    soe REAL,
                    PRIMARY KEY (gateway_id, ts)
                );
                CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts);
                CREATE TABLE IF NOT EXISTS daily_energy (
                    gateway_id TEXT NOT NULL,
                    day TEXT NOT NULL,
                    solar_kwh REAL NOT NULL DEFAULT 0,
                    home_kwh REAL NOT NULL DEFAULT 0,
                    battery_charge_kwh REAL NOT NULL DEFAULT 0,
                    battery_discharge_kwh REAL NOT NULL DEFAULT 0,
                    grid_import_kwh REAL NOT NULL DEFAULT 0,
                    grid_export_kwh REAL NOT NULL DEFAULT 0,
                    last_sample_ts REAL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (gateway_id, day)
                );
                CREATE TABLE IF NOT EXISTS integration_state (
                    gateway_id TEXT PRIMARY KEY,
                    ts REAL NOT NULL,
                    solar_w REAL NOT NULL DEFAULT 0,
                    home_w REAL NOT NULL DEFAULT 0,
                    battery_charge_w REAL NOT NULL DEFAULT 0,
                    battery_discharge_w REAL NOT NULL DEFAULT 0,
                    grid_import_w REAL NOT NULL DEFAULT 0,
                    grid_export_w REAL NOT NULL DEFAULT 0
                );
                """)
            conn.commit()
            self._conn = conn
            logger.debug("TimeSeriesStore opened %s (WAL mode)", self._db_path)
        return self._conn

    # ------------------------------------------------------------------
    # Sample recording + integration
    # ------------------------------------------------------------------

    async def record_sample(
        self,
        gateway_id: str,
        ts: float,
        solar_w: float,
        home_w: float,
        battery_w: float,
        site_w: float,
        soe: Optional[float] = None,
        timezone: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Record one power sample and integrate it into daily totals.

        Battery and grid power are split into directional components using
        the PowerwallData sign conventions (battery positive = discharging,
        site positive = importing) so charge/discharge and import/export are
        never netted together.

        Args:
            gateway_id: Gateway identifier.
            ts:         Unix timestamp of the sample.
            solar_w:    Solar production (W).
            home_w:     Home/load consumption (W).
            battery_w:  Battery power (W, positive = discharging).
            site_w:     Grid power (W, positive = importing).
            soe:        Battery state of energy (%) if known.
            timezone:   Gateway timezone name for local-midnight aggregation.

        Returns:
            The updated daily-energy row for the gateway's current local day
            (or None when the store is disabled / sample skipped).
        """
        if not self.enabled:
            return None
        values = {
            "solar": max(0.0, float(solar_w or 0.0)),
            "home": max(0.0, float(home_w or 0.0)),
            "battery_charge": max(0.0, -float(battery_w or 0.0)),
            "battery_discharge": max(0.0, float(battery_w or 0.0)),
            "grid_import": max(0.0, float(site_w or 0.0)),
            "grid_export": max(0.0, -float(site_w or 0.0)),
        }
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._ensure_executor(),
            partial(
                self._record_sample_sync, gateway_id, float(ts), values, soe, timezone
            ),
        )

    def _record_sample_sync(
        self,
        gateway_id: str,
        ts: float,
        values: Dict[str, float],
        soe: Optional[float],
        timezone: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            try:
                conn = self._ensure_conn()
                state = self._load_state(gateway_id, conn)

                # Integrate against the previous sample unless this one is
                # out-of-order/duplicate (dt <= 0) — those still get stored
                # as raw rows but never move integration state backwards.
                daily_deltas: Dict[str, Dict[str, float]] = {}
                if state is not None and ts > state["ts"]:
                    daily_deltas = self._integrate_interval(state, ts, values, timezone)

                conn.execute(
                    "INSERT OR REPLACE INTO samples "
                    "(gateway_id, ts, solar_w, home_w, battery_charge_w, "
                    "battery_discharge_w, grid_import_w, grid_export_w, soe) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        gateway_id,
                        ts,
                        values["solar"],
                        values["home"],
                        values["battery_charge"],
                        values["battery_discharge"],
                        values["grid_import"],
                        values["grid_export"],
                        soe,
                    ),
                )
                for day, deltas in daily_deltas.items():
                    conn.execute(
                        "INSERT INTO daily_energy "
                        "(gateway_id, day, solar_kwh, home_kwh, battery_charge_kwh, "
                        "battery_discharge_kwh, grid_import_kwh, grid_export_kwh, "
                        "last_sample_ts, updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?) "
                        "ON CONFLICT(gateway_id, day) DO UPDATE SET "
                        "solar_kwh=solar_kwh+excluded.solar_kwh, "
                        "home_kwh=home_kwh+excluded.home_kwh, "
                        "battery_charge_kwh=battery_charge_kwh"
                        "+excluded.battery_charge_kwh, "
                        "battery_discharge_kwh=battery_discharge_kwh"
                        "+excluded.battery_discharge_kwh, "
                        "grid_import_kwh=grid_import_kwh+excluded.grid_import_kwh, "
                        "grid_export_kwh=grid_export_kwh+excluded.grid_export_kwh, "
                        "last_sample_ts=excluded.last_sample_ts, "
                        "updated_at=excluded.updated_at",
                        (
                            gateway_id,
                            day,
                            deltas["solar"],
                            deltas["home"],
                            deltas["battery_charge"],
                            deltas["battery_discharge"],
                            deltas["grid_import"],
                            deltas["grid_export"],
                            ts,
                            time.time(),
                        ),
                    )
                if state is None or ts > state["ts"]:
                    conn.execute(
                        "INSERT OR REPLACE INTO integration_state "
                        "(gateway_id, ts, solar_w, home_w, battery_charge_w, "
                        "battery_discharge_w, grid_import_w, grid_export_w) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        (
                            gateway_id,
                            ts,
                            values["solar"],
                            values["home"],
                            values["battery_charge"],
                            values["battery_discharge"],
                            values["grid_import"],
                            values["grid_export"],
                        ),
                    )
                    self._state[gateway_id] = {"ts": ts, "values": dict(values)}
                conn.commit()

                if daily_deltas:
                    zone = _get_zone(timezone)
                    today = _local_date(ts, zone).isoformat()
                    if today in daily_deltas:
                        return self._get_day_row(gateway_id, today, conn)
                return None
            except Exception as e:  # storage must never break polling
                self._note_write_failure(e)
                return None

    def _note_write_failure(self, exc: Exception) -> None:
        """Count a failed write; warn at most once per interval."""
        self._write_failures += 1
        now = time.time()
        if now - self._last_failure_warn >= _FAILURE_WARN_INTERVAL:
            self._last_failure_warn = now
            logger.warning(
                "TimeSeriesStore sample write failed (%d total): %s",
                self._write_failures,
                exc,
            )

    def _load_state(
        self, gateway_id: str, conn: sqlite3.Connection
    ) -> Optional[Dict[str, Any]]:
        """Last integrated sample for a gateway (memory cache -> DB)."""
        if gateway_id in self._state:
            return self._state[gateway_id]
        row = conn.execute(
            "SELECT * FROM integration_state WHERE gateway_id=?", (gateway_id,)
        ).fetchone()
        if row is None:
            return None
        state = {
            "ts": row["ts"],
            "values": {
                "solar": row["solar_w"],
                "home": row["home_w"],
                "battery_charge": row["battery_charge_w"],
                "battery_discharge": row["battery_discharge_w"],
                "grid_import": row["grid_import_w"],
                "grid_export": row["grid_export_w"],
            },
        }
        self._state[gateway_id] = state
        return state

    @staticmethod
    def _integrate_interval(
        state: Dict[str, Any],
        ts: float,
        values: Dict[str, float],
        timezone: Optional[str],
    ) -> Dict[str, Dict[str, float]]:
        """Trapezoidal integration of one interval, split at local midnights.

        Returns {day: {category: kWh accrued on that local day}}.
        Skips integration entirely across gaps longer than GAP_THRESHOLD.
        """
        t0 = state["ts"]
        dt = ts - t0
        if dt <= 0 or dt > GAP_THRESHOLD:
            return {}

        zone = _get_zone(timezone)
        segments = [t0] + _midnights_between(t0, ts, zone) + [ts]
        result: Dict[str, Dict[str, float]] = {}
        for a, b in zip(segments, segments[1:]):
            # Segment [a, b] accrues to the local date of its start; a
            # segment ending exactly at midnight belongs to the day before.
            day = _local_date(a, zone).isoformat()
            f0 = (a - t0) / dt
            f1 = (b - t0) / dt
            day_deltas = result.setdefault(
                day, {category: 0.0 for category in CATEGORIES}
            )
            for category in CATEGORIES:
                v0 = state["values"][category]
                v1 = values[category]
                watts0 = v0 + (v1 - v0) * f0
                watts1 = v0 + (v1 - v0) * f1
                day_deltas[category] += (watts0 + watts1) / 2.0 * (b - a) / 3_600_000.0
        return result

    def _get_day_row(
        self, gateway_id: str, day: str, conn: sqlite3.Connection
    ) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM daily_energy WHERE gateway_id=? AND day=?",
            (gateway_id, day),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_daily_energy(
        self, days: int = 7, gateway: Optional[str] = None
    ) -> Dict[str, Any]:
        """Daily energy totals, most recent day first.

        Args:
            days:    Number of days to include (counting back from today).
            gateway: Restrict to one gateway ID (None = all gateways).
        """
        if not self.enabled:
            return {"enabled": False, "days": [], "last_updated": None}
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._ensure_executor(),
            partial(self._get_daily_energy_sync, days, gateway),
        )

    def _get_daily_energy_sync(
        self, days: int, gateway: Optional[str]
    ) -> Dict[str, Any]:
        with self._lock:
            try:
                conn = self._ensure_conn()
            except sqlite3.Error as e:
                logger.debug("TimeSeriesStore query failed: %s", e)
                return {"enabled": True, "days": [], "last_updated": None}
            # Days are stored as gateway-local dates, which can trail or lead
            # the UTC date around midnight. Widen the SQL cutoff by one day so
            # late-local-day rows are never dropped, then trim to `days` after
            # grouping (ISO day strings sort correctly across gateways).
            cutoff = (datetime.now(_UTC) - timedelta(days=max(days, 1))).strftime(
                "%Y-%m-%d"
            )
            if gateway:
                rows = conn.execute(
                    "SELECT * FROM daily_energy WHERE day>=? AND gateway_id=? "
                    "ORDER BY day DESC",
                    (cutoff, gateway),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM daily_energy WHERE day>=? ORDER BY day DESC",
                    (cutoff,),
                ).fetchall()
            by_day: Dict[str, Dict[str, Dict[str, Any]]] = {}
            last_updated: Optional[float] = None
            for row in rows:
                entry = dict(row)
                by_day.setdefault(row["day"], {})[row["gateway_id"]] = entry
                if row["last_sample_ts"]:
                    last_updated = max(last_updated or 0.0, row["last_sample_ts"])
            return {
                "enabled": True,
                "days": [
                    {"day": day, "gateways": gateways}
                    for day, gateways in sorted(
                        by_day.items(), key=lambda item: item[0], reverse=True
                    )[:days]
                ],
                "last_updated": last_updated,
            }

    async def get_today(
        self, gateway_id: str, timezone: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Today's running totals for one gateway (gateway-local day)."""
        if not self.enabled:
            return None
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._ensure_executor(), partial(self._get_today_sync, gateway_id, timezone)
        )

    def _get_today_sync(
        self, gateway_id: str, timezone: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        zone = _get_zone(timezone)
        today = _local_date(time.time(), zone).isoformat()
        with self._lock:
            try:
                conn = self._ensure_conn()
            except Exception:
                return None
            return self._get_day_row(gateway_id, today, conn)

    async def get_trend(
        self,
        hours: int = 24,
        gateway: Optional[str] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
        fit: bool = False,
    ) -> Dict[str, Any]:
        """Bucketed time series for charting.

        Raw 5s samples are averaged into ~240-second buckets (per gateway,
        then summed across gateways) so a 24-hour window returns ~360 points
        instead of ~17k rows. Power columns keep the PowerwallData sign
        convention (battery positive = discharging, grid positive =
        importing) and are returned in kW; ``battery_level`` is the mean
        state of energy (%) in each bucket. Battery level lives only in raw
        samples — it is never downsampled into daily aggregates.

        Args:
            hours:   Window length (1-168) when no explicit ``start``.
            gateway: Restrict to one gateway ID (None = all, summed).
            start:   Explicit window start (epoch seconds). Overrides
                     ``hours``.
            end:     Explicit window end (epoch seconds); default now.
            fit:     Fit the window to the full range of retained raw data
                     (earliest → latest sample, per gateway when filtered).
        """
        if not self.enabled:
            return {"enabled": False, "points": [], "count": 0}
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._ensure_executor(),
            partial(self._get_trend_sync, hours, gateway, start, end, fit),
        )

    def _get_trend_sync(
        self,
        hours: int,
        gateway: Optional[str],
        start: Optional[float] = None,
        end: Optional[float] = None,
        fit: bool = False,
    ) -> Dict[str, Any]:
        hours = max(1, min(int(hours), 168))
        now = time.time()
        if start is None:
            span = hours * 3600.0
            start = now - span
        end = end or (now + 300.0)  # +300s tolerance for clock skew
        # No clamp to now: raw samples may sit slightly in the future
        # (clock skew / rounding) and excluding them drops live buckets.
        span = max(60.0, end - start)
        # Target ~360 buckets, rounded to a whole minute, never below 60s.
        bucket = max(60.0, round(span / 360.0 / 60.0) * 60.0)
        with self._lock:
            try:
                conn = self._ensure_conn()
            except sqlite3.Error as e:
                logger.debug("TimeSeriesStore query failed: %s", e)
                return {
                    "enabled": True,
                    "points": [],
                    "count": 0,
                    "bucket_seconds": bucket,
                    "hours": hours,
                    "start": start,
                    "end": end,
                }
            if fit:
                # Fit spans the full range of retained raw data (per gateway
                # when filtered); span floor below handles the 1-sample case.
                row = conn.execute(
                    "SELECT MIN(ts) AS lo, MAX(ts) AS hi FROM samples"
                    + (" WHERE gateway_id=?" if gateway else ""),
                    ((gateway,) if gateway else ()),
                ).fetchone()
                if row is not None and row["lo"] is not None:
                    start = float(row["lo"])
                    end = float(row["hi"])
            span = max(60.0, end - start)
            bucket = max(60.0, round(span / 360.0 / 60.0) * 60.0)
            # Inner query: mean per (bucket, gateway) so multi-gateway setups
            # sum instead of average; outer query collapses to fleet totals
            # (mean SoE). Single-gateway deployments get plain bucket means.
            # Standalone solar-inverter gateways (type: "inverter") are excluded
            # from fleet solar sums if Powerwall gateways are present, to avoid
            # double-counting solar already measured by Powerwalls. If only inverters
            # exist (no Powerwalls), inverters are included.
            from app.core.gateway_manager import gateway_manager

            inverter_gw_ids = [
                gid
                for gid, gw in gateway_manager.gateways.items()
                if gw and gw.type == "inverter"
            ]
            if not inverter_gw_ids:
                inverter_gw_ids = [
                    gid
                    for gid, status in gateway_manager.cache.items()
                    if status.gateway and status.gateway.type == "inverter"
                ]

            has_powerwalls = any(
                gw and gw.type != "inverter"
                for gw in gateway_manager.gateways.values()
            ) or any(
                status.gateway and status.gateway.type != "inverter"
                for status in gateway_manager.cache.values()
            )

            gw_filter = "AND gateway_id=? " if gateway else ""
            inverter_filter = ""
            if not gateway and inverter_gw_ids and has_powerwalls:
                placeholders = ",".join("?" for _ in inverter_gw_ids)
                inverter_filter = f"AND gateway_id NOT IN ({placeholders}) "

            sql = (
                "SELECT bstart, "
                "SUM(solar_avg)/1000.0 AS solar_kw, "
                "SUM(home_avg)/1000.0 AS home_kw, "
                "SUM(batt_avg)/1000.0 AS battery_kw, "
                "SUM(grid_avg)/1000.0 AS grid_kw, "
                "AVG(soe_avg) AS battery_level "
                "FROM (SELECT (CAST(ts/? AS INTEGER))*? AS bstart, "
                "gateway_id, AVG(solar_w) AS solar_avg, "
                "AVG(home_w) AS home_avg, "
                "AVG(battery_discharge_w - battery_charge_w) AS batt_avg, "
                "AVG(grid_import_w - grid_export_w) AS grid_avg, "
                "AVG(soe) AS soe_avg FROM samples WHERE ts>=? AND ts<=? "
                + gw_filter
                + inverter_filter
                + "GROUP BY bstart, gateway_id) "
                "GROUP BY bstart ORDER BY bstart"
            )
            params = [bucket, bucket, start, end]
            if gateway:
                params.append(gateway)
            elif inverter_gw_ids and has_powerwalls:
                params.extend(inverter_gw_ids)

            rows = conn.execute(sql, tuple(params)).fetchall()
            points = [
                {
                    "ts": row["bstart"],
                    "solar_kw": row["solar_kw"],
                    "home_kw": row["home_kw"],
                    "battery_kw": row["battery_kw"],
                    "grid_kw": row["grid_kw"],
                    "battery_level": row["battery_level"],
                }
                for row in rows
            ]
            return {
                "enabled": True,
                "hours": hours,
                "bucket_seconds": bucket,
                "start": start,
                "end": end,
                "points": points,
                "count": len(points),
                "last_updated": now,
            }

    async def get_samples(
        self,
        gateway: Optional[str] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
        limit: int = 500,
    ) -> Dict[str, Any]:
        """Raw samples, ascending by time (for troubleshooting).

        Args:
            gateway: Restrict to one gateway ID (None = all).
            start:   Inclusive start timestamp (unix).
            end:     Inclusive end timestamp (unix).
            limit:   Maximum rows to return (capped at 10,000).
        """
        if not self.enabled:
            return {"enabled": False, "samples": [], "count": 0}
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._ensure_executor(),
            partial(self._get_samples_sync, gateway, start, end, limit),
        )

    def _get_samples_sync(
        self,
        gateway: Optional[str],
        start: Optional[float],
        end: Optional[float],
        limit: int,
    ) -> Dict[str, Any]:
        limit = max(1, min(int(limit), 10_000))
        with self._lock:
            try:
                conn = self._ensure_conn()
            except sqlite3.Error as e:
                logger.debug("TimeSeriesStore query failed: %s", e)
                return {"enabled": True, "samples": [], "count": 0}
            clauses, params = [], []
            if gateway:
                clauses.append("gateway_id=?")
                params.append(gateway)
            if start is not None:
                clauses.append("ts>=?")
                params.append(float(start))
            if end is not None:
                clauses.append("ts<=?")
                params.append(float(end))
            where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = conn.execute(
                f"SELECT * FROM samples{where} ORDER BY ts DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
            samples = [dict(row) for row in reversed(rows)]
            return {"enabled": True, "samples": samples, "count": len(samples)}

    async def status(self) -> Dict[str, Any]:
        """Subsystem status for /api/timeseries/status and the UI."""
        if not self.enabled:
            return {
                "enabled": False,
                "retention_seconds": -1,
                "daily_retention_seconds": self._daily_retention,
                "db_file": None,
                "db_size_bytes": 0,
                "samples": 0,
                "daily_rows": 0,
                "write_failures": 0,
                "gateways": [],
            }
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._ensure_executor(), self._status_sync)

    def _status_sync(self) -> Dict[str, Any]:
        db_size = 0
        samples = daily_rows = 0
        gateways: List[str] = []
        with self._lock:
            if Path(self._db_path).exists():
                db_size = os.path.getsize(self._db_path)
                wal = Path(self._db_path + "-wal")
                if wal.exists():
                    db_size += wal.stat().st_size
                try:
                    conn = self._ensure_conn()
                    samples = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
                    daily_rows = conn.execute(
                        "SELECT COUNT(*) FROM daily_energy"
                    ).fetchone()[0]
                    gateways = [
                        row[0]
                        for row in conn.execute(
                            "SELECT DISTINCT gateway_id FROM integration_state"
                        ).fetchall()
                    ]
                except sqlite3.Error as e:
                    logger.debug("TimeSeriesStore status query failed: %s", e)
        return {
            "enabled": True,
            "retention_seconds": self._retention,
            "daily_retention_seconds": self._daily_retention,
            # Filename only — the unauthenticated status endpoint must not
            # disclose filesystem layout (matches the /stats masking policy).
            "db_file": os.path.basename(self._db_path),
            "db_size_bytes": db_size,
            "samples": samples,
            "daily_rows": daily_rows,
            "write_failures": self._write_failures,
            "gateways": gateways,
        }

    # ------------------------------------------------------------------
    # Maintenance (pruning)
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background maintenance loop (no-op when disabled)."""
        if not self.enabled:
            return
        if self._maintenance_task is None or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(
                self._maintenance_loop(), name="timeseries-maintenance"
            )
            logger.info(
                "TimeSeriesStore enabled — raw retention %ss, daily retention %ss, "
                "db: %s",
                self._retention,
                self._daily_retention,
                self._db_path,
            )

    async def _maintenance_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(MAINTENANCE_INTERVAL)
                await self.maintenance()
            except asyncio.CancelledError:
                break
            except Exception as e:  # pragma: no cover - defensive
                logger.debug("TimeSeriesStore maintenance error: %s", e)

    async def maintenance(self) -> None:
        """Prune raw samples and stale daily aggregates, checkpoint WAL."""
        if not self.enabled:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._ensure_executor(), self._maintenance_sync)

    def _maintenance_sync(self) -> None:
        with self._lock:
            try:
                conn = self._ensure_conn()
                now = time.time()
                if self._retention > 0:
                    # Raw samples older than the retention window go away,
                    # but the last RAW_KEEP_FLOOR of data always stays.
                    cutoff = now - max(self._retention, RAW_KEEP_FLOOR)
                    conn.execute("DELETE FROM samples WHERE ts < ?", (cutoff,))
                if self._daily_retention > 0:
                    cutoff_day = (
                        datetime.fromtimestamp(now, _UTC)
                        - timedelta(seconds=self._daily_retention)
                    ).strftime("%Y-%m-%d")
                    conn.execute(
                        "DELETE FROM daily_energy WHERE day < ?", (cutoff_day,)
                    )
                conn.commit()
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.Error as e:
                logger.debug("TimeSeriesStore maintenance failed: %s", e)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def stop(self) -> None:
        """Stop maintenance and close the database. Safe to call repeatedly."""
        if self._maintenance_task and not self._maintenance_task.done():
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass
            self._maintenance_task = None
        loop = asyncio.get_running_loop()
        if loop.is_running():
            await loop.run_in_executor(None, self._close_sync)
        else:  # pragma: no cover - defensive
            self._close_sync()

    def _close_sync(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    self._conn.close()
                except sqlite3.Error as e:
                    logger.debug("TimeSeriesStore close failed: %s", e)
                self._conn = None
            self._state.clear()
        if self._executor is not None:
            # cancel_futures: queued writes must not reopen the closed DB
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None


# module-level singleton, built lazily from settings (never at import time,
# so disabled deployments and tests never touch the filesystem)
_store: Optional[TimeSeriesStore] = None


def get_timeseries_store() -> TimeSeriesStore:
    """Return the process-wide TimeSeriesStore built from settings."""
    global _store
    if _store is None:
        from app.config import settings  # late import — avoids import cycle

        _store = TimeSeriesStore(
            db_path=settings.timeseries_path,
            retention=settings.timeseries_retention,
            daily_retention=settings.timeseries_daily_retention,
        )
    return _store


def reset_timeseries_store() -> None:
    """Close and drop the singleton (used by tests and config reloads).

    Cancels the maintenance task without awaiting (callers are typically
    synchronous teardown paths), then closes SQLite synchronously.
    """
    global _store
    if _store is not None:
        if _store._maintenance_task and not _store._maintenance_task.done():
            _store._maintenance_task.cancel()
        _store._close_sync()  # pylint: disable=protected-access
        _store = None
