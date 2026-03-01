# Releases and tagging

This project follows a simple, human-driven release process for portfolio/demo purposes.

Suggested tags:
- `v0.1.0` — initial MVP (protocol, append-only logs, metadata, idempotence, metrics)
- `v0.2.0` — storage/durability and segmentation improvements
- `v0.3.0` — simple replication and replication tests

How to create a release locally and push a tag:

```bash
# create annotated tag
git tag -a v0.2.0 -m "v0.2.0: storage durability and segmentation"
git push origin v0.2.0
```

You can draft a GitHub release from the tag in the repository's Releases UI.
