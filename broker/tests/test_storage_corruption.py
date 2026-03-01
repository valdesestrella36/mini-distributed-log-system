import sys
from pathlib import Path
import tempfile
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker


def test_read_messages_skips_corrupted_lines():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        b = Broker(data_dir=data_dir)

        # write a log file with: valid, corrupted, valid
        lp = data_dir / "ctopic_0.log"
        lp.parent.mkdir(parents=True, exist_ok=True)
        with lp.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"value": "good1"}) + "\n")
            f.write("{this is not json}\n")
            f.write(json.dumps({"value": "good2"}) + "\n")

        msgs = b._read_messages("ctopic", 0, 0, 4096)
        values = [m["value"] for m in msgs]
        assert "good1" in values
        assert "good2" in values
        assert len(values) == 2
