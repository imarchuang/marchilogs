# Compaction design (v0 — minimal)

Aligned with VictoriaLogs: **per-day partitions**, **immutable parts**, background merge of many small parts into fewer larger ones. No cold storage in this doc (comes after retention + compaction).

## Current state (problem)

Every `Flush` / `InmemoryDataFlushInterval` tick publishes a new numeric part under:

```
partitions/YYYYMMDD/parts/000001/
partitions/YYYYMMDD/parts/000002/
...
```

Without merge, part count grows ≈ `ingest_duration / flush_interval`. Search lists and opens many parts → FD / IO / meta overhead explode.

## Goals (v0)

1. Bound the number of **small** parts per day partition.
2. Produce larger **compacted** parts that replace the inputs atomically (readers never see half-merged data).
3. Run in background; never block `Append` except briefly for metadata swap.
4. Stay single-process; no cluster.

Non-goals for v0: tiering to object storage, force-merge HTTP (can add later), cross-day merge, delete-by-query.

## Part tiers (names)

| Tier | How created | Typical size | Role |
|---|---|---|---|
| **mem** | buffers / inmemory scan | RAM | tip; already queryable |
| **small** | flush of mem → disk | small (flush batch) | durable, many files |
| **big** | merge of small (and later big+big) | larger | fewer files, better scan |

On disk we can keep one directory layout and mark tier in `meta.json`:

```json
{ "tier": "small" | "big", "time_min_ns": ..., "streams": [...], "by_tag": {...}, "blocks": [...] }
```

v0 may use directory prefix instead: `parts/s-000001` vs `parts/b-000001`, or keep numeric ids and only use `meta.tier`. Prefer **`meta.tier` + existing numeric ids** to avoid breaking `isPublishedPartDir`.

## When to merge (trigger)

Per day partition, independently:

```
if count(small parts) >= MergeMinParts        // e.g. 4
   OR total_small_bytes >= MergeMinBytes      // optional
then schedule merge of the oldest N small parts (e.g. 2..8)
```

Also:

- Periodic ticker (e.g. same order as flush, or `MergeCheckInterval` default 10s).
- On `Close`: optional final merge (nice-to-have, not required).

Cap concurrency: **one merge at a time globally** in v0 (simplest). Later: one per partition.

## Merge algorithm (v0)

Input: set of published **small** parts `P1..Pk` in one day partition (chosen by policy).

1. Create tmp dir `parts/.merging-<outId>/` (same atomic-publish idea as flush).
2. Stream-merge by stream id (and time within stream):
   - Union stream tag index (`by_tag`).
   - For each stream that appears in any Pi, concatenate / rewrite columnar blocks into new block(s) with updated time ranges.
3. Write new `meta.json` with `tier: "big"`, combined time range, merged `by_tag`.
4. `rename` tmp → `parts/<newId>/` (published).
5. Atomically update a **partition manifest** (see below) **or** delete input part dirs only after new part is visible.
6. Delete `P1..Pk`.

Readers (`Search`) must either:

- **A (v0 simple):** only list final numeric dirs; during merge, both old and new may briefly exist → **duplicate rows** if not careful; therefore prefer **B**, or  
- **B (recommended):** maintain `partitions/YYYYMMDD/manifest.json` listing **active** part ids; Search only opens ids in the manifest; merge swaps manifest in one rename.

### Manifest (recommended for v0)

```json
{
  "parts": ["000001", "000002", "000007"]
}
```

- Flush: write part → append id to manifest via write-temp + rename.
- Merge: write new part → manifest' = manifest − inputs + newId → rename.
- Search / Open: read manifest; ignore stray dirs (crash leftovers cleaned on Open).

This reuses the same “publish then appear” discipline as flush.

## Interaction with existing pieces

| Component | Interaction |
|---|---|
| `InmemoryDataFlushInterval` | Produces **small** parts; compaction consumes them |
| WAL (opt-in) | Unrelated to merge; still checkpoints on flush only |
| Stream index / block time refs | Must be rebuilt into output part meta (same format) |
| RLock search | Merge should not hold write lock for the whole rewrite; only for manifest swap |

## Config (proposed)

| Option | Default | Meaning |
|---|---|---|
| `MergeMinParts` | `4` | Start merge when small-part count ≥ this |
| `MergeMaxPartsPerJob` | `8` | Max inputs per merge |
| `MergeCheckInterval` | `10s` | Background check period; `≤0` disables |
| `MergeDisable` | false | Hard off |

HTTP later: `POST /internal/force_merge?day=YYYYMMDD` (VL-like).

## Failure / crash

| Crash point | Recovery |
|---|---|
| During `.merging-*` write | Open deletes `.merging-*` / `.publishing-*`; manifest unchanged |
| After new part rename, before manifest swap | Open: part exists but not in manifest → delete orphan part **or** ignore |
| After manifest swap, before deleting inputs | Open: inputs not in manifest → delete orphans |

## Implementation order

1. **Manifest** for active parts (flush + search switch to manifest).  
2. **Merge worker** (small→big, one job, rewrite columns naively).  
3. **Metrics**: `parts_small`, `parts_big`, `merge_duration`, `rows_merged`.  
4. Force-merge API + tests (dup-free Search under concurrent flush/merge).

## Test plan (when coding)

- Flush until `MergeMinParts`, wait for merge → part count drops, Search row count unchanged.  
- Crash mid-merge → Open cleans tmp; data intact.  
- Concurrent Append + Search during merge → no dupes (manifest).  
- Sabotage: remove an input part’s `.col` after merge scheduled but before delete — should not affect readers if manifest already swapped.

## Out of scope → next docs

- **Retention**: drop whole day dirs / cap disk.  
- **Cold storage**: move old **big** parts (or whole days) to object storage; manifest gains `location`.  
- **Bloom**: build during merge into big parts (amortized cost).
