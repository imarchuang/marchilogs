# Durability window (VictoriaLogs-style)

By default marchilogs does **not** use a WAL. Unflushed buffers are made durable by
periodically flushing them to disk parts.

| Setting | Default | Notes |
|---|---|---|
| `InmemoryDataFlushInterval` | `5s` | Guaranteed flush of in-memory data to parts |
| minimum | `1s` | Values in `(0, 1s)` are raised to `1s` |
| disable | `< 0` | e.g. `-1` for tests |

This matches VictoriaLogs `-inmemoryDataFlushInterval`: trade a short loss window on
unclean shutdown (OOM / kill / power loss) for lower write amplification than
per-Append `fsync` WAL.

Optional `EnableWAL` can still be turned on for stronger durability of the tip.

HTTP flag: `-inmemoryDataFlushInterval` / `MARCHILOGS_INMEMORY_DATA_FLUSH_INTERVAL`
(`0` disables periodic flush).
