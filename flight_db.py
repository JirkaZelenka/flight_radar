"""SQLite úložiště pro OpenSky REST API (/states/all).

Dvě tabulky:
  aircraft      — jedno letadlo = jeden icao24 (transpondér)
  observations  — každý snapshot z API callu (pozice, výška, rychlost, …)

Časy se ukládají jako UTC text: 2026-07-29 20:17:06.395581
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

DB_PATH = Path(__file__).resolve().parent / "flights.sqlite"
DATETIME_FMT = "%Y-%m-%d %H:%M:%S.%f"

SCHEMA = """
CREATE TABLE IF NOT EXISTS aircraft (
    icao24            TEXT PRIMARY KEY,
    callsign          TEXT,
    origin_country    TEXT,
    category          INTEGER,
    first_seen        TEXT NOT NULL,
    last_seen         TEXT NOT NULL,
    observation_count INTEGER NOT NULL DEFAULT 0,
    note              TEXT
);

CREATE TABLE IF NOT EXISTS observations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    icao24           TEXT NOT NULL REFERENCES aircraft(icao24),
    snapshot_time    TEXT NOT NULL,
    ingested_at      TEXT NOT NULL,
    callsign         TEXT,
    origin_country   TEXT,
    time_position    TEXT,
    last_contact     TEXT,
    longitude        REAL,
    latitude         REAL,
    baro_altitude    REAL,
    on_ground        INTEGER,
    velocity         REAL,
    true_track       REAL,
    vertical_rate    REAL,
    sensors          TEXT,
    geo_altitude     REAL,
    squawk           TEXT,
    spi              INTEGER,
    position_source  INTEGER,
    category         INTEGER,
    UNIQUE (icao24, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_obs_icao24 ON observations(icao24);
CREATE INDEX IF NOT EXISTS idx_obs_snapshot_time ON observations(snapshot_time);
"""

TIMESTAMP_COLUMNS = {
    "aircraft": ("first_seen", "last_seen"),
    "observations": ("snapshot_time", "ingested_at", "time_position", "last_contact"),
}

# OpenSky /states/all — indexy v poli state vectoru
# https://openskynetwork.github.io/opensky-api/rest.html
IDX_ICAO24 = 0
IDX_CALLSIGN = 1
IDX_ORIGIN_COUNTRY = 2
IDX_TIME_POSITION = 3
IDX_LAST_CONTACT = 4
IDX_LONGITUDE = 5
IDX_LATITUDE = 6
IDX_BARO_ALTITUDE = 7
IDX_ON_GROUND = 8
IDX_VELOCITY = 9
IDX_TRUE_TRACK = 10
IDX_VERTICAL_RATE = 11
IDX_SENSORS = 12
IDX_GEO_ALTITUDE = 13
IDX_SQUAWK = 14
IDX_SPI = 15
IDX_POSITION_SOURCE = 16
IDX_CATEGORY = 17

OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
ENV_PATH = Path(__file__).resolve().parent / ".env"

# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30

# Standard authenticated quota is 4,000/day. Active feeder (≥30% uptime)
# gets 8,000. Remaining > 4,000 at the start of a day confirms feeder tier.
STANDARD_DAILY_CREDITS = 4000
FEEDER_DAILY_CREDITS = 8000

# latitude = severní–jižní šířka, longitude = východní–západní délka
DEFAULT_STATES_PARAMS: dict[str, Any] = {
    "lamin": 48.4,
    "lamax": 51.2,
    "lomin": 11.8,
    "lomax": 19.0,
    "extended": 1,  # včetně aircraft category
}


def format_timestamp(value: Any) -> str | None:
    """Unix timestamp / datetime / ISO string -> '2026-07-29 20:17:06.395581' (UTC)."""
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime(DATETIME_FMT)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text[:4].isdigit() and text[4:5] == "-":
            cleaned = text.replace(" UTC", "").replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt.strftime(DATETIME_FMT)
        try:
            value = float(text)
        except ValueError:
            return text

    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        return (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            .replace(tzinfo=None)
            .strftime(DATETIME_FMT)
        )

    return None


def connect(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = connect(db_path)
    conn.executescript(SCHEMA)
    _migrate_timestamps(conn)
    _ensure_aircraft_note(conn)
    conn.commit()
    return conn


def _table_column_types(conn: sqlite3.Connection, table: str) -> dict[str, str]:
    return {
        row["name"]: (row["type"] or "").upper()
        for row in conn.execute(f"PRAGMA table_info({table})")
    }


def _ensure_aircraft_note(conn: sqlite3.Connection) -> None:
    types = _table_column_types(conn, "aircraft")
    if types and "note" not in types:
        conn.execute("ALTER TABLE aircraft ADD COLUMN note TEXT")


def _migrate_timestamps(conn: sqlite3.Connection) -> None:
    """Převede unix integer časy na text a případně změní typ sloupců na TEXT."""
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }

    for table, columns in TIMESTAMP_COLUMNS.items():
        if table not in tables:
            continue
        col_sql = ", ".join(columns)
        rows = conn.execute(f"SELECT rowid AS _rid, {col_sql} FROM {table}").fetchall()
        for row in rows:
            updates: dict[str, str] = {}
            for col in columns:
                old = row[col]
                new = format_timestamp(old)
                if new is not None and new != old and str(old) != new:
                    updates[col] = new
            if updates:
                set_clause = ", ".join(f"{col} = :{col}" for col in updates)
                conn.execute(
                    f"UPDATE {table} SET {set_clause} WHERE rowid = :rid",
                    {**updates, "rid": row["_rid"]},
                )

    if "aircraft" not in tables:
        return
    if _table_column_types(conn, "aircraft").get("first_seen") == "TEXT":
        return

    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.executescript(
        """
        PRAGMA foreign_keys = OFF;
        DROP TABLE IF EXISTS aircraft_v2;
        DROP TABLE IF EXISTS observations_v2;

        CREATE TABLE aircraft_v2 (
            icao24            TEXT PRIMARY KEY,
            callsign          TEXT,
            origin_country    TEXT,
            category          INTEGER,
            first_seen        TEXT NOT NULL,
            last_seen         TEXT NOT NULL,
            observation_count INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO aircraft_v2 (
            icao24, callsign, origin_country, category,
            first_seen, last_seen, observation_count
        )
        SELECT
            icao24, callsign, origin_country, category,
            first_seen, last_seen, observation_count
        FROM aircraft;
        DROP TABLE aircraft;
        ALTER TABLE aircraft_v2 RENAME TO aircraft;

        CREATE TABLE observations_v2 (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            icao24           TEXT NOT NULL REFERENCES aircraft(icao24),
            snapshot_time    TEXT NOT NULL,
            ingested_at      TEXT NOT NULL,
            callsign         TEXT,
            origin_country   TEXT,
            time_position    TEXT,
            last_contact     TEXT,
            longitude        REAL,
            latitude         REAL,
            baro_altitude    REAL,
            on_ground        INTEGER,
            velocity         REAL,
            true_track       REAL,
            vertical_rate    REAL,
            sensors          TEXT,
            geo_altitude     REAL,
            squawk           TEXT,
            spi              INTEGER,
            position_source  INTEGER,
            category         INTEGER,
            UNIQUE (icao24, snapshot_time)
        );
        INSERT INTO observations_v2 SELECT * FROM observations;
        DROP TABLE observations;
        ALTER TABLE observations_v2 RENAME TO observations;

        CREATE INDEX IF NOT EXISTS idx_obs_icao24 ON observations(icao24);
        CREATE INDEX IF NOT EXISTS idx_obs_snapshot_time ON observations(snapshot_time);
        """
    )
    conn.execute("PRAGMA foreign_keys = ON")


def _strip_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_int_bool(value: Any) -> int | None:
    if value is None:
        return None
    return 1 if value else 0


def _parse_state(state: list[Any]) -> dict[str, Any] | None:
    if not state or not state[IDX_ICAO24]:
        return None

    category = state[IDX_CATEGORY] if len(state) > IDX_CATEGORY else None
    sensors = state[IDX_SENSORS]
    sensors_json = json.dumps(sensors) if sensors is not None else None

    return {
        "icao24": str(state[IDX_ICAO24]).strip().lower(),
        "callsign": _strip_or_none(state[IDX_CALLSIGN]),
        "origin_country": _strip_or_none(state[IDX_ORIGIN_COUNTRY]),
        "time_position": format_timestamp(state[IDX_TIME_POSITION]),
        "last_contact": format_timestamp(state[IDX_LAST_CONTACT]),
        "longitude": state[IDX_LONGITUDE],
        "latitude": state[IDX_LATITUDE],
        "baro_altitude": state[IDX_BARO_ALTITUDE],
        "on_ground": _as_int_bool(state[IDX_ON_GROUND]),
        "velocity": state[IDX_VELOCITY],
        "true_track": state[IDX_TRUE_TRACK],
        "vertical_rate": state[IDX_VERTICAL_RATE],
        "sensors": sensors_json,
        "geo_altitude": state[IDX_GEO_ALTITUDE],
        "squawk": _strip_or_none(state[IDX_SQUAWK]),
        "spi": _as_int_bool(state[IDX_SPI]),
        "position_source": state[IDX_POSITION_SOURCE],
        "category": category,
    }


def ingest_states(payload: dict[str, Any], db_path: Path | str = DB_PATH) -> dict[str, Any]:
    """Uloží odpověď OpenSky REST API. Upsert letadel + insert nových pozorování.

    Stejný (icao24, snapshot_time) se znovu nevkládá — opakovaný call
    se stejným OpenSky timestampem observation nepřidá.
    """
    conn = init_db(db_path)
    snapshot_time = format_timestamp(payload.get("time") or time.time())
    ingested_at = format_timestamp(datetime.now(timezone.utc))
    states = payload.get("states") or []

    aircraft_upserted = 0
    observations_inserted = 0
    observations_skipped = 0

    try:
        for state in states:
            parsed = _parse_state(state)
            if parsed is None:
                continue

            conn.execute(
                """
                INSERT INTO aircraft (
                    icao24, callsign, origin_country, category,
                    first_seen, last_seen, observation_count
                )
                VALUES (:icao24, :callsign, :origin_country, :category,
                        :seen, :seen, 0)
                ON CONFLICT(icao24) DO UPDATE SET
                    callsign = COALESCE(excluded.callsign, aircraft.callsign),
                    origin_country = COALESCE(excluded.origin_country, aircraft.origin_country),
                    category = COALESCE(excluded.category, aircraft.category),
                    last_seen = MAX(aircraft.last_seen, excluded.last_seen)
                """,
                {**parsed, "seen": snapshot_time},
            )
            aircraft_upserted += 1

            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO observations (
                    icao24, snapshot_time, ingested_at,
                    callsign, origin_country, time_position, last_contact,
                    longitude, latitude, baro_altitude, on_ground,
                    velocity, true_track, vertical_rate, sensors,
                    geo_altitude, squawk, spi, position_source, category
                )
                VALUES (
                    :icao24, :snapshot_time, :ingested_at,
                    :callsign, :origin_country, :time_position, :last_contact,
                    :longitude, :latitude, :baro_altitude, :on_ground,
                    :velocity, :true_track, :vertical_rate, :sensors,
                    :geo_altitude, :squawk, :spi, :position_source, :category
                )
                """,
                {**parsed, "snapshot_time": snapshot_time, "ingested_at": ingested_at},
            )

            if cursor.rowcount:
                observations_inserted += 1
                conn.execute(
                    """
                    UPDATE aircraft
                    SET observation_count = observation_count + 1
                    WHERE icao24 = ?
                    """,
                    (parsed["icao24"],),
                )
            else:
                observations_skipped += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "snapshot_time": snapshot_time,
        "states_in_response": len(states),
        "aircraft_upserted": aircraft_upserted,
        "observations_inserted": observations_inserted,
        "observations_skipped": observations_skipped,
    }


def fetch_aircraft(db_path: Path | str = DB_PATH) -> list[sqlite3.Row]:
    conn = init_db(db_path)
    try:
        return conn.execute(
            "SELECT * FROM aircraft ORDER BY last_seen DESC"
        ).fetchall()
    finally:
        conn.close()


def fetch_observations(
    icao24: str | None = None,
    limit: int = 50,
    db_path: Path | str = DB_PATH,
) -> list[sqlite3.Row]:
    conn = init_db(db_path)
    try:
        if icao24:
            return conn.execute(
                """
                SELECT * FROM observations
                WHERE icao24 = ?
                ORDER BY snapshot_time DESC
                LIMIT ?
                """,
                (icao24.lower(), limit),
            ).fetchall()
        return conn.execute(
            """
            SELECT * FROM observations
            ORDER BY snapshot_time DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()


def fetch_snapshot_times(db_path: Path | str = DB_PATH) -> list[dict[str, Any]]:
    """Seznam uložených snapshotů, nejnovější první."""
    conn = init_db(db_path)
    try:
        rows = conn.execute(
            """
            SELECT snapshot_time AS time, COUNT(*) AS count
            FROM observations
            GROUP BY snapshot_time
            ORDER BY snapshot_time DESC
            """
        ).fetchall()
        return [{"time": row["time"], "count": row["count"]} for row in rows]
    finally:
        conn.close()


def _observation_to_state(row: sqlite3.Row) -> list[Any]:
    sensors = row["sensors"]
    if sensors:
        try:
            sensors = json.loads(sensors)
        except json.JSONDecodeError:
            pass
    on_ground = row["on_ground"]
    spi = row["spi"]
    return [
        row["icao24"],
        row["callsign"],
        row["origin_country"],
        row["time_position"],
        row["last_contact"],
        row["longitude"],
        row["latitude"],
        row["baro_altitude"],
        None if on_ground is None else bool(on_ground),
        row["velocity"],
        row["true_track"],
        row["vertical_rate"],
        sensors,
        row["geo_altitude"],
        row["squawk"],
        None if spi is None else bool(spi),
        row["position_source"],
        row["category"],
    ]


def fetch_snapshot(snapshot_time: str, db_path: Path | str = DB_PATH) -> dict[str, Any]:
    """Vrátí jeden snapshot ve tvaru OpenSky {time, states}."""
    conn = init_db(db_path)
    try:
        rows = conn.execute(
            """
            SELECT * FROM observations
            WHERE snapshot_time = ?
            ORDER BY id
            """,
            (snapshot_time,),
        ).fetchall()
        return {
            "time": snapshot_time,
            "states": [_observation_to_state(row) for row in rows],
        }
    finally:
        conn.close()


def format_state_line(parsed: dict[str, Any]) -> str:
    """Jedno letadlo na řádek — stejný formát jako v notebooku."""
    callsign = parsed.get("callsign") or "UNKNOWN"
    on_ground = parsed.get("on_ground")
    spi = parsed.get("spi")
    return (
        f"{callsign:10} {parsed.get('icao24')} "
        f"lat={parsed.get('latitude')} lon={parsed.get('longitude')} "
        f"baro={parsed.get('baro_altitude')} m geo={parsed.get('geo_altitude')} m "
        f"spd={parsed.get('velocity')} m/s trk={parsed.get('true_track')}° "
        f"vrt={parsed.get('vertical_rate')} m/s "
        f"on_ground={None if on_ground is None else bool(on_ground)} "
        f"squawk={parsed.get('squawk')} "
        f"country={parsed.get('origin_country')} category={parsed.get('category')} "
        f"last_contact={parsed.get('last_contact')} "
        f"time_pos={parsed.get('time_position')} "
        f"spi={None if spi is None else bool(spi)} "
        f"pos={parsed.get('position_source')} sensors={parsed.get('sensors')}"
    )


def _load_dotenv(path: Path | str = ENV_PATH) -> None:
    """Načte KEY=VALUE z .env do os.environ (existující env má přednost)."""
    env_file = Path(path)
    if not env_file.is_file():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


def parse_rate_limit_headers(response: requests.Response) -> dict[str, Any]:
    remaining_raw = response.headers.get("X-Rate-Limit-Remaining")
    retry_raw = response.headers.get("X-Rate-Limit-Retry-After-Seconds")
    try:
        remaining = int(remaining_raw) if remaining_raw is not None else None
    except ValueError:
        remaining = None
    try:
        retry_after = int(retry_raw) if retry_raw is not None else None
    except ValueError:
        retry_after = None

    feeder_confirmed = remaining is not None and remaining > STANDARD_DAILY_CREDITS
    if remaining is None:
        note = "Hlavička X-Rate-Limit-Remaining chybí."
    elif feeder_confirmed:
        note = (
            f"X-Rate-Limit-Remaining={remaining} > {STANDARD_DAILY_CREDITS} "
            f"- feeder kvóta (~{FEEDER_DAILY_CREDITS} kreditů/den) vypadá aktivní."
        )
    else:
        note = (
            f"X-Rate-Limit-Remaining={remaining} <= {STANDARD_DAILY_CREDITS} "
            f"- standardní kvóta, nebo feeder tier ještě nenasadil "
            f"(přepočet každých ~2 h, upgrade po ~50 requestech)."
        )

    extra = {
        key: value
        for key, value in response.headers.items()
        if key.lower().startswith("x-rate-limit-")
    }
    return {
        "remaining": remaining,
        "retry_after_seconds": retry_after,
        "status_code": response.status_code,
        "feeder_confirmed": feeder_confirmed,
        "note": note,
        "headers": extra,
    }


class TokenManager:
    """Access token z OpenSky OpenID (client_credentials), s automatickým refreshem."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        _load_dotenv()
        self.client_id = client_id or os.environ.get("CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "Chybí CLIENT_ID / CLIENT_SECRET. Doplň je do souboru .env."
            )
        self.token: str | None = None
        self.expires_at: datetime | None = None

    def get_token(self) -> str:
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def invalidate(self) -> None:
        """Drop the cached token so the next call fetches a new one."""
        self.token = None
        self.expires_at = None

    def _refresh(self) -> str:
        """Fetch a new access token from the OpenSky authentication server."""
        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        self.token = data["access_token"]
        expires_in = int(data.get("expires_in", 1800))
        # Never schedule expiry in the past if the server sends a short TTL.
        refresh_after = max(1, expires_in - TOKEN_REFRESH_MARGIN)
        self.expires_at = datetime.now() + timedelta(seconds=refresh_after)
        return self.token

    def headers(self) -> dict[str, str]:
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}


_tokens: TokenManager | None = None


def get_tokens() -> TokenManager:
    """Jedna sdílená instance TokenManager pro celý modul."""
    global _tokens
    if _tokens is None:
        _tokens = TokenManager()
    return _tokens


class OpenSkyClient:
    """Klient OpenSky `/states/all` pro snadné volání z notebooku.

    params       — bbox / extended; None = DEFAULT_STATES_PARAMS (Česko + okolí)
    save_to_db   — True = hned uložit odpověď přes ingest_states()
    """

    def __init__(
        self,
        params: dict[str, Any] | None = None,
        save_to_db: bool = False,
        db_path: Path | str = DB_PATH,
    ) -> None:
        self.params = {**DEFAULT_STATES_PARAMS, **(params or {})}
        self.save_to_db = save_to_db
        self.db_path = Path(db_path)
        self.tokens = get_tokens()
        self.last_data: dict[str, Any] | None = None
        self.last_stats: dict[str, Any] | None = None
        self.last_rate_limit: dict[str, Any] | None = None

    def _get_states(self, *, _retried: bool = False) -> requests.Response:
        response = requests.get(
            OPENSKY_STATES_URL,
            params=self.params,
            headers=self.tokens.headers(),
            timeout=30,
        )
        self.last_rate_limit = parse_rate_limit_headers(response)
        # Official docs: 401 means the access token expired — fetch a new one and retry.
        if response.status_code == 401 and not _retried:
            self.tokens.invalidate()
            return self._get_states(_retried=True)
        if response.status_code == 429:
            retry = (self.last_rate_limit or {}).get("retry_after_seconds")
            raise RuntimeError(
                f"OpenSky 429: kredity vyčerpány, retry after {retry}s"
            )
        response.raise_for_status()
        return response

    def fetch(self) -> dict[str, Any]:
        """Stáhne aktuální stavy. Při save_to_db uloží do SQLite. Vrací raw JSON."""
        response = self._get_states()
        data = response.json()
        self.last_data = data

        if self.save_to_db:
            stats = ingest_states(data, db_path=self.db_path)
            stats["saved_to_db"] = True
        else:
            states = data.get("states") or []
            stats = {
                "snapshot_time": format_timestamp(data.get("time") or time.time()),
                "states_in_response": len(states),
                "aircraft_upserted": 0,
                "observations_inserted": 0,
                "observations_skipped": 0,
                "saved_to_db": False,
            }

        stats["rate_limit"] = self.last_rate_limit
        self.last_stats = stats
        return data

    def check_quota(self) -> dict[str, Any]:
        """Monitorovací call: Bearer GET /states/all a výpis X-Rate-Limit-Remaining.

        Active feeder status se přepočítává každých ~2 h. Tier upgrade se projeví
        po ~50 requestech. Feeder kvóta 8 000 je potvrzená, když remaining
        na začátku dne překročí 4 000.
        """
        self._get_states()
        self.print_quota()
        return self.last_rate_limit or {}

    def print_quota(self) -> None:
        quota = self.last_rate_limit
        if not quota:
            print("Žádné rate-limit údaje — nejdřív zavolej fetch() nebo check_quota().")
            return
        remaining = quota.get("remaining")
        print(quota["note"])
        print(
            f"remaining={remaining}  "
            f"feeder_confirmed={quota.get('feeder_confirmed')}  "
            f"HTTP {quota.get('status_code')}  "
            f"retry_after={quota.get('retry_after_seconds')}"
        )
        extra = quota.get("headers") or {}
        if extra:
            for key, value in extra.items():
                print(f"  {key}: {value}")

    def parsed_states(self, data: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        payload = data if data is not None else self.last_data
        if not payload:
            return []
        parsed: list[dict[str, Any]] = []
        for state in payload.get("states") or []:
            row = _parse_state(state)
            if row is not None:
                parsed.append(row)
        return parsed

    def print_states(
        self,
        data: dict[str, Any] | None = None,
        *,
        print_raw: bool = False,
    ) -> None:
        """Vypíše souhrn a každé letadlo. Bez předchozího fetch() neudělá nic."""
        payload = data if data is not None else self.last_data
        stats = self.last_stats if data is None else None
        if payload is None:
            print("Žádná data — nejdřív zavolej fetch().")
            return

        if stats is None:
            states = payload.get("states") or []
            stats = {
                "snapshot_time": format_timestamp(payload.get("time")),
                "states_in_response": len(states),
                "observations_inserted": "—",
                "observations_skipped": "—",
                "saved_to_db": self.save_to_db,
            }

        print(
            f"OpenSky time={stats['snapshot_time']}  "
            f"letadel v odpovědi={stats['states_in_response']}  "
            f"nových pozorování={stats.get('observations_inserted', '—')}  "
            f"přeskočeno (stejný snapshot)={stats.get('observations_skipped', '—')}  "
            f"uloženo do DB={stats.get('saved_to_db', self.save_to_db)}"
        )
        if self.last_rate_limit:
            remaining = self.last_rate_limit.get("remaining")
            print(
                f"kredity remaining={remaining}  "
                f"feeder_confirmed={self.last_rate_limit.get('feeder_confirmed')}"
            )

        for row in self.parsed_states(payload):
            print(format_state_line(row))

        if print_raw:
            print(payload)
