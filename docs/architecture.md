# Arquitectura (MVP)

- `broker/`: servidor de red que maneja requests y persiste logs por topic/partition.
- `clients/`: `producer` y `consumer` como CLIs que hablan con el broker por TCP.
- `storage`: append-only logs por (topic, partition) en disco (línea por mensaje en MVP).
- `metadata`: SQLite para registrar topics/partitions (fase 1).

Concurrency: `asyncio` en el broker para manejar muchas conexiones concurrentes.
