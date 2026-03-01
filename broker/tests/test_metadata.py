import sys
from pathlib import Path
import asyncio
import tempfile
import sqlite3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker


def test_metadata_created_on_produce():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        b = Broker(data_dir=data_dir)
        # produce a message (synchronously via internal method)
        off = b._append_message("meta_topic", 0, "v1")
        assert off == 0
        # check meta DB exists and has topic
        meta_db = data_dir.parent / "meta.db"
        assert meta_db.exists()
        conn = sqlite3.connect(str(meta_db))
        cur = conn.cursor()
        cur.execute("SELECT name FROM topics WHERE name = ?", ("meta_topic",))
        row = cur.fetchone()
        conn.close()
        assert row is not None and row[0] == "meta_topic"
