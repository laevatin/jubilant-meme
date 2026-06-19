# blobstore — a log-structured binary blob store (C++17)

Write an arbitrary-length binary string, get back an **index**; load it back later by
that index. Built for concurrent stores/loads, crash recovery, and a read cache that
coalesces duplicate loads.

This is the same family as **Bitcask**, **Kafka log segments**, and a DB **WAL**:
an append-only segmented log + cheap handles + checksummed records for recovery.

## Locked design trade-offs

| Axis | Choice | Consequence |
|------|--------|-------------|
| **Index semantics** | Opaque packed handle `(segment, offset, length)` | O(1) reads, no index to persist, trivial recovery. **Never** relocate data → no compaction/GC/delete-reclaim. |
| **Durability** | Group commit (batched `fsync`) | High throughput under concurrency; a crash may lose the last few ms of *un-synced* acknowledged writes. `sync()` forces a flush. |
| **Layout** | Segmented append-only log, no GC | Sealed segments accumulate; write-once. Simple, append-fast. |
| **Read cache** | Sharded LRU + single-flight | Duplicate concurrent loads of the same index collapse onto one `pread`. Configurable capacity/shards; disable for raw benchmarks. |

Scope limit: **arbitrary length is supported**, but the tuned/measured path is **4K and 1M** blocks.

## On-disk record framing

```
+--------+--------+--------+------------------+--------+
| magic  |  len   | crc32c |     payload      | footer |
| u32    |  u32   |  u32   |   len bytes      |  u32   |   footer == len
+--------+--------+--------+------------------+--------+
  \__________ 12-byte header _________/                  4-byte tail
```

* `crc32c` covers the payload. On load we read `12 + len + 4` bytes in **one `pread`**
  (the handle already carries `len`), then verify magic + crc + footer.
* **Recovery** scans the tail of the active segment; the first record that fails magic/crc/
  footer/bounds marks the truncation point (torn write from a crash). Sealed segments are trusted.

## Index (the returned handle)

16 bytes: `segment:u32` (1-indexed; 0 = invalid sentinel), `length:u32`, `offset:u64`.
`Index::bytes()` / `Index::from_bytes()` serialize it so callers can persist handles.

## Concurrency model

* **Stores** serialize only on a tiny offset-reservation critical section (no I/O held);
  the `pwrite` happens outside the lock. Durability is batched by a background syncer thread.
* **Loads** need no lock against writers — records are immutable once written. The segment-fd
  registry is read-mostly (`shared_mutex`); the cache is sharded.

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
