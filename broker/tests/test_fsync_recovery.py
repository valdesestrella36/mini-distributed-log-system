import os

from broker import Broker


def test_fsync_persistence_after_restart(tmp_path):
    os.environ["MDLS_FSYNC"] = "1"
    data_dir = tmp_path / "data"
    b = Broker(data_dir=data_dir)

    # append a message synchronously
    off = b._append_message("dur-topic", 0, "important", None)
    assert isinstance(off, int)

    # simulate restart by creating a fresh Broker instance pointing to same data
    b2 = Broker(data_dir=data_dir)
    msgs = b2._read_messages("dur-topic", 0, 0, 4096)
    assert any(m.get("value") == "important" for m in msgs)
