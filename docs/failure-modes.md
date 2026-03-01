# Failure modes (visión rápida)

- Broker crash: mensajes ya escritos en disco persisten; en fase 2 replicación mitigará pérdida.
- Partial write: usar CRC y truncation (mejora futura).
- Network partitions: reintentos y ack semantics (fase 2).
