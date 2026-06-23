# blobstore — a log-structured binary blob store (C++17)

Write an arbitrary-length binary string, get back an **index**; load it back later by
that index. Built for concurrent stores/loads, crash recovery, and a read cache that
coalesces duplicate loads.

This is the same family as **Bitcask**, **Kafka log segments**, and a DB **WAL**:
a single append-only log + cheap handles + checksummed records for recovery.

## Locked design trade-offs

| Axis | Choice | Consequence |
|------|--------|-------------|
| **Index semantics** | Opaque packed handle `(segment, length, offset)` | O(1) reads, no index to persist, trivial recovery. **Never** relocate data → no compaction/GC/delete-reclaim. |
| **Durability** | Four modes: `Sync`, `GroupCommit` (default), `AsyncFlush`, `OsBuffered` | `Sync`/`GroupCommit` are durable-before-return (group amortizes one `fsync` across concurrent writers). `AsyncFlush` returns after the `pwrite` and a background worker `fsync`s on a size **or** time threshold — lowest latency, bounded loss window. `sync()` forces a flush. |
| **Layout** | One append-only segment file, no GC | A single write-once file; `segment` is fixed at 1. Caps a store at <4 GiB (offset must fit the cache key); `store()` throws when full. |
| **Read cache** | Sharded LRU + single-flight | Duplicate concurrent loads of the same index collapse onto one `pread`. Configurable capacity/shards; disable for raw benchmarks. |
| **Batch API** | `appendBatch` / `loadBatch` | One offset reservation, one contiguous `pwrite`, and **one durability barrier** amortized across the whole batch. |

Scope limit: **arbitrary length is supported**, but the tuned/measured path is **4K and 1M** blocks.

## On-disk record framing

```
+--------+--------+--------+------------------+--------+
| magic  |  len   | crc32c |     payload      | footer |
| u32    |  u32   |  u32   |   len bytes      |  u32   |   footer == len
+--------+--------+--------+------------------+--------+
  \__________ 12-byte header _________/                  4-byte tail
```

* `crc32c` covers the **header (magic+len) and the payload** — a flip anywhere in the
  framing, not just the payload, is caught at `load()`. On load we read `12 + len + 4`
  bytes in **one `pread`** (the handle carries `len`), then verify magic + crc + footer.
* **Recovery** scans from offset 0 for the last *framing-sound* record (magic ok, fits the
  file, header `len` == footer `len`). The first record that fails framing is a torn tail
  (crash mid-append) → truncated. A record whose framing is sound but whose **CRC** fails is
  interior bit-rot, not a tear: it is left in place (rejected at `load()`) and the scan steps
  over it, so corruption of one record never discards the records written after it.

## Index (the returned handle)

16 bytes: `segment:u32` (always 1; 0 = invalid sentinel), `length:u32`, `offset:u64`.
`Index::bytes()` / `Index::from_bytes()` serialize it so callers can persist handles.

## Concurrency model

* **Stores** serialize only on a tiny offset-reservation critical section (no I/O held);
  the `pwrite` happens outside the lock. Durability is batched by a background syncer thread.
* **Loads** need no lock against writers — records are immutable once written, and the single
  segment fd is fixed after open; the cache is sharded.

## Layout

```
include/   crc32c.h  lru_cache.h  blob_store.h     (public API + headers)
src/       blob_store.cpp
test/      test_framework.h  test_*.cpp            (TDD: written before impl)
bench/     bench.cpp                               (random/seq x read/write x 4K/1M)
```

## Build & test

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
ctest --test-dir build --output-on-failure
./build/bench --help
```
