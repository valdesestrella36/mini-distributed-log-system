import sqlite3
from pathlib import Path
from typing import List
import threading
import datetime


class Metadata:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # allow usage from different threads
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS topics (
                    name TEXT PRIMARY KEY,
                    created_at TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS partitions (
                    topic TEXT,
                    partition INTEGER,
                    PRIMARY KEY(topic, partition)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dedup (
                    topic TEXT,
                    partition INTEGER,
                    message_id TEXT,
                    offset INTEGER,
                    PRIMARY KEY(topic, partition, message_id)
                )
                """
            )
            self._conn.commit()

    def add_topic(self, name: str, partitions: int = 1) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("INSERT OR IGNORE INTO topics(name, created_at) VALUES(?, ?)", (name, now))
            for p in range(partitions):
                cur.execute("INSERT OR IGNORE INTO partitions(topic, partition) VALUES(?, ?)", (name, p))
            self._conn.commit()

    def lookup_message_id(self, topic: str, partition: int, message_id: str):
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT offset FROM dedup WHERE topic = ? AND partition = ? AND message_id = ? LIMIT 1",
                (topic, partition, message_id),
            )
            row = cur.fetchone()
            return row[0] if row is not None else None

    def add_message_id(self, topic: str, partition: int, message_id: str, offset: int) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO dedup(topic, partition, message_id, offset) VALUES(?, ?, ?, ?)",
                (topic, partition, message_id, offset),
            )
            self._conn.commit()

    def has_topic(self, name: str) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT 1 FROM topics WHERE name = ? LIMIT 1", (name,))
            return cur.fetchone() is not None

    def list_partitions(self, name: str) -> List[int]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT partition FROM partitions WHERE topic = ? ORDER BY partition", (name,))
            return [row[0] for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass
