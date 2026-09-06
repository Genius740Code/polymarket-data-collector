"""Crash/restart durable cursor store — §1B.

Persists per-asset (or shared WAL) cursor state every 5-10s and on shutdown.

Concurrency (§1B):
- per_asset: one SQLite file per asset → no lock contention, crash isolation
- shared_wal: single SQLite file in WAL mode → concurrent writers don't serialize

Fields per asset: current_window_index, current_condition_id, next_condition_id,
last_sequence_number_per_token, last_snapshot_written_ts
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class CursorState:
    asset: str
    current_window_index: int = 0
    current_condition_id: Optional[str] = None
    next_condition_id: Optional[str] = None
    last_sequence_number_per_token: Dict[str, int] = field(default_factory=dict)
    last_snapshot_written_ts: Optional[int] = None  # unix ms
    updated_at: Optional[str] = None
    window_label: str = "5m"  # timeframe lane (5m/15m/1h/4h/1d) — multi-TF keying

    def to_row(self) -> tuple:
        # column order matches the INSERT in save(): window_label second
        return (
            self.asset,
            self.window_label,
            self.current_window_index,
            self.current_condition_id,
            self.next_condition_id,
            json.dumps(self.last_sequence_number_per_token),
            self.last_snapshot_written_ts,
            self.updated_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )

    @classmethod
    def from_row(cls, row: tuple) -> "CursorState":
        # column order matches SELECT in load()/load_all(): window_label second
        asset, window_label, widx, cur_cid, nxt_cid, seq_json, last_ts, updated = row
        try:
            seqs = json.loads(seq_json) if seq_json else {}
        except Exception:
            seqs = {}
        # json keys are strings, values ints
        seqs = {str(k): int(v) for k, v in seqs.items()}
        return cls(
            asset=asset,
            current_window_index=int(widx) if widx is not None else 0,
            current_condition_id=cur_cid,
            next_condition_id=nxt_cid,
            last_sequence_number_per_token=seqs,
            last_snapshot_written_ts=last_ts,
            updated_at=updated,
            window_label=window_label or "5m",
        )


_DDL = """
CREATE TABLE IF NOT EXISTS cursor_state (
    asset TEXT NOT NULL,
    window_label TEXT NOT NULL DEFAULT '5m',
    current_window_index INTEGER NOT NULL,
    current_condition_id TEXT,
    next_condition_id TEXT,
    last_sequence_number_per_token TEXT NOT NULL DEFAULT '{}',
    last_snapshot_written_ts INTEGER,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (asset, window_label)
);
"""


class CursorStore:
    """Durable cursor store for one asset (per_asset mode) or shared WAL.

    Usage:
        store = CursorStore.for_asset(config, "BTC")
        store.save(state)
        state = store.load("BTC")
    """

    def __init__(self, db_path: Path, wal_mode: bool = False):
        self.db_path = Path(db_path)
        self.wal_mode = wal_mode
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0, check_same_thread=False)
        if self.wal_mode:
            # WAL mode must be set outside transaction; journal_mode pragma
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        else:
            conn.execute("PRAGMA journal_mode=DELETE;")
            conn.execute("PRAGMA synchronous=FULL;")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            # Legacy migration: pre-multi-TF databases have cursor_state keyed by
            # asset alone. Preserve those rows as the 5m lane, then adopt the new
            # (asset, window_label) schema.
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cursor_state'")
            if cur.fetchone() is not None:
                cols = [r[1] for r in conn.execute("PRAGMA table_info(cursor_state)").fetchall()]
                if "window_label" not in cols:
                    conn.executescript(
                        """
                        ALTER TABLE cursor_state RENAME TO cursor_state_legacy;
                        CREATE TABLE cursor_state (
                            asset TEXT NOT NULL,
                            window_label TEXT NOT NULL DEFAULT '5m',
                            current_window_index INTEGER NOT NULL,
                            current_condition_id TEXT,
                            next_condition_id TEXT,
                            last_sequence_number_per_token TEXT NOT NULL DEFAULT '{}',
                            last_snapshot_written_ts INTEGER,
                            updated_at TEXT NOT NULL,
                            PRIMARY KEY (asset, window_label)
                        );
                        INSERT INTO cursor_state
                          (asset, window_label, current_window_index, current_condition_id,
                           next_condition_id, last_sequence_number_per_token, last_snapshot_written_ts, updated_at)
                        SELECT asset, '5m', current_window_index, current_condition_id,
                           next_condition_id, last_sequence_number_per_token, last_snapshot_written_ts, updated_at
                        FROM cursor_state_legacy;
                        DROP TABLE cursor_state_legacy;
                        """
                    )
            conn.executescript(_DDL)
            conn.commit()
        finally:
            conn.close()

    def save(self, state: CursorState) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO cursor_state
                  (asset, window_label, current_window_index, current_condition_id, next_condition_id,
                   last_sequence_number_per_token, last_snapshot_written_ts, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset, window_label) DO UPDATE SET
                  current_window_index=excluded.current_window_index,
                  current_condition_id=excluded.current_condition_id,
                  next_condition_id=excluded.next_condition_id,
                  last_sequence_number_per_token=excluded.last_sequence_number_per_token,
                  last_snapshot_written_ts=excluded.last_snapshot_written_ts,
                  updated_at=excluded.updated_at
                """,
                state.to_row(),
            )
            conn.commit()
        finally:
            conn.close()

    def load(self, asset: str, window_label: str = "5m") -> Optional[CursorState]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT asset, window_label, current_window_index, current_condition_id, next_condition_id, last_sequence_number_per_token, last_snapshot_written_ts, updated_at FROM cursor_state WHERE asset=? AND window_label=?",
                (asset.upper(), str(window_label).lower()),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return CursorState.from_row(row)
        finally:
            conn.close()

    def load_all(self) -> Dict[tuple, CursorState]:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT asset, window_label, current_window_index, current_condition_id, next_condition_id, last_sequence_number_per_token, last_snapshot_written_ts, updated_at FROM cursor_state")
            return {(r[0], r[1] or "5m"): CursorState.from_row(r) for r in cur.fetchall()}
        finally:
            conn.close()

    def sync(self) -> None:
        """Compatibility alias — flush durable state. SQLite is synchronous on save(),
        so this is a no-op checkpoint that ensures WAL is checkpointed if needed."""
        if self.wal_mode:
            try:
                conn = self._connect()
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                pass

    # -- factory helpers (§1B concurrency spec) -----------------------------
    @classmethod
    def for_asset(cls, config, asset: str) -> "CursorStore":
        """Create store for a single asset respecting config.cursor_store.mode."""
        mode = getattr(config.cursor_store, "mode", "per_asset")
        base = Path(config.cursor_store.path)
        if mode == "per_asset":
            return cls(base / f"{asset.upper()}.db", wal_mode=False)
        elif mode == "shared_wal":
            return cls(base / "shared.db", wal_mode=True)
        else:
            raise ValueError(f"Unknown cursor_store.mode {mode}")

    @classmethod
    def shared(cls, config) -> "CursorStore":
        """Create the single shared WAL store (only when mode==shared_wal)."""
        base = Path(config.cursor_store.path)
        return cls(base / "shared.db", wal_mode=True)
