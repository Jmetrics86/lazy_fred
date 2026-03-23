"""
Persistent local database for lazy_fred — tracks every download run,
stores observations, and reconciles re-pulled historical data.

Uses SQLite so there are zero external dependencies.
"""

import datetime
import json
import os
import sqlite3

DB_FILENAME = "lazy_fred_history.db"
CONFIG_DIR = ".lazy_fred_configs"


# ── SQLite data store ─────────────────────────────────────────────────────────

class DataStore:
    """Manages a local SQLite database that remembers every download."""

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or os.path.join(os.getcwd(), DB_FILENAME)
        self._conn: sqlite3.Connection | None = None
        self._ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _ensure_tables(self):
        conn = self._connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                config_name TEXT,
                series_json TEXT,
                lookback    TEXT,
                series_count INTEGER,
                new_rows    INTEGER DEFAULT 0,
                updated_rows INTEGER DEFAULT 0,
                status      TEXT    DEFAULT 'started'
            );

            CREATE TABLE IF NOT EXISTS observations (
                series_id   TEXT NOT NULL,
                date        TEXT NOT NULL,
                value       REAL,
                frequency   TEXT,
                first_pulled TEXT NOT NULL,
                last_pulled  TEXT NOT NULL,
                run_id      INTEGER,
                PRIMARY KEY (series_id, date)
            );

            CREATE INDEX IF NOT EXISTS idx_obs_series
                ON observations(series_id);
            CREATE INDEX IF NOT EXISTS idx_obs_date
                ON observations(date);
        """)
        conn.commit()

    # ── Runs ──────────────────────────────────────────────────────────────

    def start_run(self, series_ids: list[str], lookback: str | None,
                  config_name: str | None = None) -> int:
        conn = self._connect()
        cur = conn.execute(
            """INSERT INTO runs
               (timestamp, config_name, series_json, lookback, series_count)
               VALUES (?, ?, ?, ?, ?)""",
            (datetime.datetime.now().isoformat(timespec="seconds"),
             config_name,
             json.dumps(series_ids),
             lookback,
             len(series_ids)),
        )
        conn.commit()
        return cur.lastrowid

    def finish_run(self, run_id: int, new_rows: int, updated_rows: int):
        conn = self._connect()
        conn.execute(
            """UPDATE runs
               SET status='completed', new_rows=?, updated_rows=?
               WHERE id=?""",
            (new_rows, updated_rows, run_id),
        )
        conn.commit()

    def get_recent_runs(self, limit: int = 10) -> list[dict]:
        conn = self._connect()
        cur = conn.execute(
            """SELECT id, timestamp, config_name, series_count,
                      lookback, new_rows, updated_rows, status
               FROM runs ORDER BY id DESC LIMIT ?""",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── Observations ──────────────────────────────────────────────────────

    def upsert_observations(self, rows: list[tuple], run_id: int,
                            frequency: str) -> tuple[int, int]:
        """Insert or update observations. Returns (new_count, updated_count).

        *rows* is a list of (series_id, date_str, value) tuples.
        """
        conn = self._connect()
        now = datetime.datetime.now().isoformat(timespec="seconds")
        new_count = 0
        updated_count = 0

        for series_id, date_str, value in rows:
            try:
                val = float(value) if value is not None else None
            except (ValueError, TypeError):
                val = None

            cur = conn.execute(
                "SELECT value FROM observations "
                "WHERE series_id=? AND date=?",
                (series_id, str(date_str)),
            )
            existing = cur.fetchone()

            if existing is None:
                conn.execute(
                    """INSERT INTO observations
                       (series_id, date, value, frequency,
                        first_pulled, last_pulled, run_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (series_id, str(date_str), val,
                     frequency, now, now, run_id),
                )
                new_count += 1
            else:
                conn.execute(
                    """UPDATE observations
                       SET value=?, last_pulled=?, run_id=?
                       WHERE series_id=? AND date=?""",
                    (val, now, run_id, series_id, str(date_str)),
                )
                updated_count += 1

        conn.commit()
        return new_count, updated_count

    def get_series_date_range(self, series_id: str) -> dict | None:
        """Return min/max dates and row count for a series, or None."""
        conn = self._connect()
        cur = conn.execute(
            """SELECT MIN(date), MAX(date), COUNT(*)
               FROM observations WHERE series_id=?""",
            (series_id,),
        )
        row = cur.fetchone()
        if row and row[2] > 0:
            return {"min_date": row[0], "max_date": row[1], "count": row[2]}
        return None

    def get_total_stats(self) -> dict:
        conn = self._connect()
        cur = conn.execute(
            """SELECT COUNT(DISTINCT series_id), COUNT(*),
                      MIN(date), MAX(date)
               FROM observations""",
        )
        row = cur.fetchone()
        return {
            "series_count": row[0] or 0,
            "observation_count": row[1] or 0,
            "min_date": row[2],
            "max_date": row[3],
        }

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


# ── Configuration save / load ─────────────────────────────────────────────────

def _config_dir() -> str:
    path = os.path.join(os.getcwd(), CONFIG_DIR)
    os.makedirs(path, exist_ok=True)
    return path


def save_config(name: str, series_ids: list[str],
                lookback: str | None, mode: str | None = None,
                min_popularity: int | None = None) -> str:
    """Save a wizard configuration to disk. Returns the file path."""
    config = {
        "name": name,
        "series_ids": series_ids,
        "lookback": lookback,
        "mode": mode,
        "min_popularity": min_popularity,
        "created": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    safe_name = "".join(c if c.isalnum() or c in "-_ " else "_"
                        for c in name).strip().replace(" ", "_")
    fname = f"{safe_name}.json"
    path = os.path.join(_config_dir(), fname)
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
    return path


def list_configs() -> list[dict]:
    """Return all saved configurations, newest first."""
    cdir = _config_dir()
    configs = []
    for fname in os.listdir(cdir):
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(cdir, fname)) as f:
                cfg = json.load(f)
            cfg["_file"] = os.path.join(cdir, fname)
            configs.append(cfg)
        except Exception:
            pass
    configs.sort(key=lambda c: c.get("created", ""), reverse=True)
    return configs


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)
