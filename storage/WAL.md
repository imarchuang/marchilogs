# WAL durability

Unflushed buffers are protected by a write-ahead log under `data/wal/`.

## Layout

```
data/wal/wal.log       # framed records (magic, version, len, JSON, crc32)
data/wal/checkpoint    # highest seq known durable in published parts
```

## Path

1. **Append** — normalize → append+fsync to `wal.log` (seq++) → apply to mem buffers  
2. **Flush / block-full** — publish part → advance `checkpoint` to continuous durable prefix → drop buffers → compact `wal.log`  
3. **Open** — replay records with `seq > checkpoint` into buffers  

## Options

| Option | Meaning |
|---|---|
| `DisableWAL` | No WAL (tests / ephemeral) |
| `WALSync` | `fsync` after each Append batch (default `true`) |

## Crash windows

- After WAL sync, before apply: recovered on replay.  
- After part publish, before checkpoint: rare duplicate risk; checkpoint is written **before** buffers are cleared, using the post-flush unflushed seq set, to minimize this.  
- Truncated tail of `wal.log` is ignored on read.
