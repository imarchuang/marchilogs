# In-memory search concurrency

## Current: option 1 — read lock, no copy

`Search` takes `s.mu.RLock()` and scans live `buffers` directly (`searchBuffersRLocked`).
`Append` / `Flush` take the write lock.

- **Pros:** Zero per-query clone; simple.
- **Cons:** A long in-memory scan holds the read lock and stalls writers.

Published disk part names are listed under the same `RLock` so a concurrent `Flush`
cannot double-count rows (mem already scanned + newly published part).

---

## Option 2 — prune before clone (not implemented)

Keep cloning only for blocks that can matter:

1. Maintain cheap per-buffer metadata (already have `timeMin`/`timeMax` + stream id/tags).
2. On `Search`, skip buffers whose day / stream tags / time range cannot match the query.
3. Clone (or scan under `RLock`) only the survivors.

**When it helps:** Narrow time windows or selective stream filters while buffers are large.

**Limit:** Broad queries still pay near-full cost.

Can be combined with option 1 (prune under `RLock`, still no clone) or with option 3
(only seal/clone hot blocks).

---

## Option 3 — COW / generational sealed views (not implemented)

Separate **mutable tip** from **immutable sealed inmemory parts**:

```
generation G:
  sealed[]   // immutable inmemoryPart; shared by concurrent Search (pointer, no copy)
  mutable    // active memBlocks receiving Append
```

- `Append` writes only `mutable`.
- When a block fills, on a timer, or on `Flush`: seal `mutable` into `sealed` (and/or publish to disk), then open a new empty `mutable`.
- `Search` atomically loads the current generation (sealed slice + optional short lock/clone of `mutable` tip only).

Column-level COW (copy a column only when Append mutates it while readers hold the old pointer) is a heavier variant; prefer **seal-by-block** first.

**When it helps:** High write rate + long-running queries that must not block Append.

**Cost:** Generation/refcount lifecycle, and care when sealing while Search still holds an old generation.
