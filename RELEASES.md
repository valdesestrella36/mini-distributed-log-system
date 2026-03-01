# Releases and tagging

This project follows a simple, human-driven release process for portfolio/demo purposes.

Suggested tags for public snapshots and portfolio releases:
- `v1.0.0` — polished snapshot: durability, segmentation, indices, replication, tests, docs

How to create a release locally and push a tag (example for `v1.0.0`):

```bash
# create annotated tag
git tag -a v1.0.0 -m "v1.0.0: polished snapshot — durability, segmentation, replication, tests"
git push origin v1.0.0
```

You can draft a GitHub release from the tag in the repository's Releases UI.
