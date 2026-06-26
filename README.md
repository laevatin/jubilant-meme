# recordstore — a log-structured binary record store (C++17)

Write an arbitrary-length binary string, get back an **index**; load it back later by
that index. The store is split into two decoupled halves that share **no in-memory
state** — they communicate only through the file on disk and the opaque handle:

* **`RecordWriter`** — owns the append path, the write cursor, durability, and crash
  recovery: `append`, `appendBatch`, `sync`, `close`.
* **`RecordReader`** — opens the same file read-only: `load`, `loadBatch`, `scan`.

A small **`main`** driver wires the two together.

This is the same family as **Bitcask**, **Kafka log segments**, and a DB **WAL**:
a single append-only log + cheap handles + checksummed records for recovery.

## Locked design trade-offs

| Axis | Choice | Consequence |
|------|--------|-------------|
| **Index semantics** | Opaque packed handle `(segment, length, offset)` | O(1) reads, no index to persist, trivial recovery. **Never** relocate data → no compaction/GC/delete-reclaim. |
| **Writer / reader split** | Two classes, no shared memory | A reader and a writer share only the file. Readers never touch writer state; the cursor and index are rederived by recovery, so there is no index file to keep in sync. |
| **Durability** | Four modes: `Sync`, `GroupCommit` (default), `AsyncFlush`, `OsBuffered` | `Sync`/`GroupCommit` are durable-before-return (group amortizes one `fsync` across concurrent writers). `AsyncFlush` returns after the `pwrite` and a background worker `fsync`s on a size **or** time threshold — lowest latency, bounded loss window. `sync()` forces a flush. |
| **Layout** | One append-only segment file, no GC | A single write-once file; `segment` is fixed at 1. Caps a store at <4 GiB (offset must fit a uint32); `append()` throws when full. |
| **Read path** | Plain `std::string` by value; cache is a no-op placeholder | Reads currently copy the payload out and go straight to disk (one `pread` + CRC). The earlier sharded-LRU + single-flight + zero-copy `shared_ptr` path was removed; `ReadCache` keeps the seam to reintroduce it without an interface change. |
| **Batch API** | `appendBatch` / `loadBatch` | One offset reservation, one contiguous `pwrite`, and **one durability barrier** amortized across the whole batch. |

Scope limit: **arbitrary length is supported**, but the tuned path is **4K and 1M** blocks.

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
* **Recovery** (run by `RecordWriter::open`) scans from offset 0 for the last
  *framing-sound* record (magic ok, fits the file, header `len` == footer `len`). The
  first record that fails framing is a torn tail (crash mid-append) → truncated. A record
  whose framing is sound but whose **CRC** fails is interior bit-rot, not a tear: it is left
  in place (rejected at `load()`) and stepped over, so corruption of one record never
  discards the records written after it.

## Index (the returned handle)

16 bytes: `segment:u32` (always 1; 0 = invalid sentinel), `length:u32`, `offset:u64`.
`Index::bytes()` / `Index::from_bytes()` serialize it so callers can persist handles.

## Concurrency model

* **Appends** serialize only on a tiny offset-reservation critical section (no I/O held);
  the `pwrite` happens outside the lock at the reserved offset, so concurrent appends never
  overlap. Durability is batched by a background syncer thread (group-commit / async).
* **Loads / scans** need no lock against the writer — records are immutable once written,
  and a reader has its own read-only fd. A `scan()` snapshots the file end and only ever
  yields complete, CRC-valid records.

## Layout

```
include/   record.h  record_writer.h  record_reader.h  read_cache.h  crc32c.h
src/       record_writer.cpp  record_reader.cpp  record_format.h (internal)
app/       main.cpp                                  (driver: write / read / scan)
test/      test_*.cpp                                (GoogleTest: functional, recovery, fuzz, concurrency)
```

## Build & test

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
ctest --test-dir build --output-on-failure
./build/main /tmp/demo            # write 5 records then read them back
```
