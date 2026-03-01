# C client (clients/c_client)

Mini C client to demonstrate low-level networking and framing (len(4 bytes BE) + JSON payload).

Build

```bash
cd clients/c_client
make
```

Usage examples

- Produce:
  ```bash
  ./c_client produce --host 127.0.0.1 --port 9000 --topic mytopic --partition 0 --value "hola"
  ```
- Fetch:
  ```bash
  ./c_client fetch --host 127.0.0.1 --port 9000 --topic mytopic --partition 0 --offset 0
  ```

Notes: this is intentionally minimal — no JSON escaping, meant for demos.
