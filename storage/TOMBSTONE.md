# Tombstone deletes (v0 — logical)

Log storage parts are **immutable**. You cannot rewrite a row inside an existing
columnar part, so “delete” must be a **new write** that marks matching rows as
gone. That mark is a **tombstone**.

In KV/LSM engines a tombstone is usually `(key → deleted)`. Logs rarely have a
stable row id; deletes are almost always **predicates over time + stream +
message**. marchilogs follows that model.

## Semantics (what Delete means)

```text
DELETE WHERE time ∈ [start,end]   -- optional bounds
      AND stream matches StreamEq -- optional tag subset
      AND _msg contains S         -- optional substring
```

After `Delete`:

1. Matching **in-memory tip** rows are removed immediately (physical purge of RAM).
2. Matching **on-disk** rows stay in their parts but become **invisible to Search**.
3. Disk space is **not** reclaimed yet; that is a later compaction / GC slice.

Retention (`-retentionPeriod`) still drops **whole day directories**. Tombstones
cover **intra-day** selective deletes.

### Non-goals (v0)

| Out of scope | Why |
|---|---|
| Per-row id delete | No stable row id in the columnar model |
| Field updates | Would need versioning; not a log MVP |
| Physical drop inside parts | Needs merge rewrite; deferred |
| Distributed delete protocol | Single-process store |
| Empty predicate (= delete everything) | Rejected to avoid foot-guns |

At least one of `start`, `end`, `stream_eq`, `contains` is required.

## Why predicate tombstones (not in-column markers)

Alternatives considered:

| Approach | Verdict |
|---|---|
| Special “deleted” rows in `_msg.col` | Couples delete to bloom / column codecs; pollutes scans |
| Per-block bitset inside each part | Requires rewriting parts on every delete |
| **Separate day-level delete records** | Chosen: one file, Search filters, compaction can GC later |

Tombstones live **beside** data parts, not inside them:

```text
partitions/YYYYMMDD/
  manifest.json
  indexdb.json          # stream → parts (unchanged)
  deletes.json          # ← tombstone list for this day
  parts/000001/...
```

### On-disk record shape

```json
{
  "deletes": [
    {
      "id": "d-000001",
      "created_ns": 1740000000000000000,
      "start_ns": 1740000000000000000,
      "end_ns": 1740086399999999999,
      "stream_eq": {"service": "api"},
      "contains": "password"
    }
  ]
}
```

- All set fields are **AND**-ed when deciding if a row is hidden.
- Unset time bounds are unbounded on that side.
- `stream_eq` is a **subset** match on `_stream` tags (same rules as Query).
- Writes use atomic rename (`deletes.json.tmp` → `deletes.json`).

## Runtime path

### Delete

1. Take write lock.
2. `purgeBuffersMatchingLocked` — drop tip rows that match the spec.
3. Resolve overlapping day partitions (`manifests` ∪ on-disk days ∪ calendar bounds).
4. Append one `deleteRecord` per day (monotonic `d-NNNNNN` id), rewrite `deletes.json`.
5. Keep an in-memory `deletes[day]` mirror for Search (loaded again on `Open`).

### Search

1. Under read lock, snapshot tombstones for days overlapping the query window.
2. Mem tip scan: after time / contains filters, skip rows `entryHiddenByDeletes`.
3. Disk scan: same per-row check after reading a block.
4. `QueryStats.RowsSuppressedDelete` counts hidden rows (also
   `X-Marchilogs-Rows-Suppressed-Delete`).

Optional later: block-level `mayHideBlock` to skip whole blocks when time+stream
prove a tombstone cannot apply (Contains still forces a scan).

### Retention

When a day directory is removed, clear `deletes[day]` / `deleteSeq[day]` with the
manifest and indexdb. The file goes away with the day tree.

## HTTP

```text
POST /delete?start=&end=&contains=&service=&host=...
```

Same stream-field query params as `/query`. Response:

```json
{"ok": true, "purged_mem": 3}
```

`purged_mem` is only the tip purge count; disk rows are suppressed, not counted
here.

## Relation to LSM “tombstone”

Same idea, different grain:

| | Classic LSM | marchilogs v0 |
|---|---|---|
| Unit | key (or key range) | time ∩ stream ∩ contains |
| Write | append delete marker to MemTable/SSTable | append predicate to `deletes.json` |
| Read | newer tombstone shadows older value | Search skips matching rows |
| Space reclaim | compaction drops covered keys | **not yet** — merge rewrite planned |

So this is still a tombstone: **logical delete via an append-only mark**;
physical reclaim is deferred to merge.

## Roadmap (later slices)

1. **Compaction GC** — when rewriting parts, drop rows that any day tombstone
   hides; shrink or drop tombstones that no longer cover remaining data.
2. **Delete GC** — expire / compact `deletes.json` once all covered parts are
   rewritten or retention removed the day.
3. **Block prune** — use `mayHideBlock` (+ bloom for Contains) to avoid reading
   columns that cannot contribute visible rows.

## Code map

| Piece | Location |
|---|---|
| Spec / persist / Delete | `storage/delete.go` |
| Search filter | `SearchWithStats`, `searchBuffersRLocked` |
| Stats field | `QueryStats.RowsSuppressedDelete` |
| HTTP | `POST /delete` in `cmd/marchilogs/main.go` |
| Tests | `storage/delete_test.go`, `TestDeleteHTTP` |
