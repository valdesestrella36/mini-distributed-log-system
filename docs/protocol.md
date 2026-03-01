# Protocolo (MVP)

Enlace por TCP con framing: 4 bytes big-endian indicando la longitud del payload seguido del payload JSON UTF-8.

Requests (JSON):

- PING: `{ "type": "PING" }`
- PRODUCE: `{ "type": "PRODUCE", "topic": "t", "partition": 0, "value": "..." }`
- FETCH: `{ "type": "FETCH", "topic": "t", "partition": 0, "offset": 0, "max_bytes": 4096 }`

Responses (JSON):

- OK: `{ "status": "OK", ... }`
- ERR: `{ "status": "ERR", "code": "..", "message": "..." }`

Notas: usar framing evita problemas con streams TCP. Mantener compatibilidad añadiendo campos opcionales.
