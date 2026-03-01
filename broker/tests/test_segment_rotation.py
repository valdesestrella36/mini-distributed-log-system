import os

from broker import Broker


def test_segment_rotation_and_read(tmp_path):
    os.environ["MDLS_SEGMENT_BYTES"] = "128"  # small to force rotation
    data_dir = tmp_path / "data"
    b = Broker(data_dir=data_dir)

    # append several messages to trigger rotation
    for i in range(20):
        b._append_message("seg-topic", 0, f"m{i}", None)

    # ensure multiple segment files exist
    segs = list(data_dir.glob("seg-topic_0*.log"))
    assert len(segs) >= 2

    # read back all messages
    b2 = Broker(data_dir=data_dir)
    msgs = b2._read_messages("seg-topic", 0, 0, 65536)
    vals = [m.get("value") for m in msgs]
    for i in range(20):
        assert f"m{i}" in vals
