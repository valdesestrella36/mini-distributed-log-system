import os

from broker import Broker


def test_segment_index_created_and_used(tmp_path):
    data_dir = tmp_path / "data"
    b = Broker(data_dir=data_dir)

    for i in range(10):
        b._append_message("idx-topic", 0, f"v{i}", None)

    # in-memory index should be present after building
    seg = next(data_dir.glob("idx-topic_0*.log"))
    b2 = Broker(data_dir=data_dir)
    assert str(seg) in b2._indices
    msgs = b2._read_messages("idx-topic", 0, 0, 65536)
    vals = [m.get("value") for m in msgs]
    for i in range(10):
        assert f"v{i}" in vals
