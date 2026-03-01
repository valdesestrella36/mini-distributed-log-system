import sys
from pathlib import Path
import tempfile
import sqlite3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker


def test_idempotent_append_using_message_id():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        b = Broker(data_dir=data_dir)
        off1 = b._append_message("id_topic", 0, "first", message_id="mid-123")
        off2 = b._append_message("id_topic", 0, "second", message_id="mid-123")
        assert off1 == 0
        assert off2 == 0
        # check only one line in log and value is the first one
        logp = data_dir / "id_topic_0.log"
        with logp.open("r", encoding="utf-8") as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
        assert len(lines) == 1
        import json

        msg = json.loads(lines[0])
        assert msg.get("value") == "first"
