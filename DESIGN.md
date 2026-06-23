# blobstore — Design Document

A C++17 store for arbitrary-length binary blobs: write a blob, get an opaque
**index**; load it back by that index. Built for concurrent loads/stores, crash
recovery, and a read cache that coalesces duplicate loads.

## Assumptions

- Workload is **write-once, read-many**; no in-place update or delete.
- Blobs are **immutable** once stored; a handle stays valid for the store's life.
- Hot block sizes are **4 KiB and 1 MiB**, but any size up to ~4 GiB is accepted.
- A single store lives on **one local POSIX filesystem**; `pwrite`/`pread`/`fsync`
  behave per POSIX. One process opens a given store directory at a time.
- The application may issue **duplicate concurrent loads** of the same index.
- Total live data per store fits in **< 4 GiB** (single-segment cap).

## Principles & key points

- **The handle is the index.** The returned `Index` encodes the record's physical
  location, so a load is one `pread` with no lookup table to persist or rebuild.
- **Append-only, never relocate.** No compaction, GC, or delete-reclaim — this keeps
  handles stable forever and makes recovery trivial.
- **Detect corruption, don't hide it.** Every record is CRC-checksummed; a load
  returns exact bytes or throws — never wrong bytes.
- **Amortize the expensive parts.** Group-commit batches `fsync`; the batch API
  amortizes locking and durability across many blobs.

## Architecture

```
        store()/appendBatch()                 load()/loadBatch()
                |                                     |
         frame + reserve offset                 cache lookup (sharded LRU
         (append_mu)                             + single-flight)
                |                                     | miss
            pwrite (lockless, positioned)        pread + verify CRC/framing
                |                                     |
         durability barrier  <-- syncer thread       shared_ptr<const string>
         (group-commit / sync / osbuffered)
                |
        one append-only segment file (NNNNNN.seg, id=1)
```

- **Append path:** frame the record(s), reserve a byte range under a tiny mutex,
  `pwrite` outside the lock, then wait for the durability barrier.
- **Syncer thread:** for group-commit, coalesces queued writers and issues one
  `fsync` per batch, then wakes them.
- **Read path:** cache hit returns immediately; a miss does one positioned `pread`,
  validates, and populates the cache.

## File format

A store directory holds one segment file `000001.seg`. Records are packed back to
back, each framed as:

```
+--------+--------+--------+--------------+--------+
| magic  |  len   | crc32c |   payload    | footer |
| u32    |  u32   |  u32   |  len bytes   |  u32   |   footer == len
+--------+--------+--------+--------------+--------+
 \_________ 12-byte header _________/                4-byte tail
```

- `magic` = `'BLOB'`; `len` = payload length; `footer` = a second copy of `len`.
- `crc32c` (Castagnoli) covers the **header (magic+len) and the payload**, so a flip
  anywhere in the framing is caught — not just in the payload.
- Little-endian on disk. A record occupies `12 + len + 4` bytes.

## Write procedure

1. Frame the blob into a buffer (`magic | len | crc | payload | footer`).
2. Under `append_mu`, reserve `[cursor, cursor+total)`; throw if it exceeds the cap;
   advance `cursor`. (Batch: reserve the whole batch's bytes once.)
3. `pwrite` the buffer at the reserved offset (no lock held).
4. Apply the durability barrier per mode:
   - **Sync** — `fsync` inline, then return.
   - **GroupCommit** — enqueue a waiter and block until the background syncer's
     batched `fsync` covers it (durable-before-return, one `fsync` per concurrent batch).
   - **AsyncFlush** — return immediately; a background worker `fsync`s once unsynced
     bytes exceed `async_flush_bytes` **or** `async_flush_interval_us` elapses.
   - **OsBuffered** — nothing until `sync()`/close.
5. Return `Index{segment=1, length, offset}`.

## Read procedure

1. If the cache is on, look up `(segment, offset)`; on a hit return the shared bytes.
   Concurrent misses for the same key collapse onto one loader (single-flight).
2. On a miss, `pread` `12 + len + 4` bytes at `offset` (handle carries `len` → one
   syscall).
3. Verify: magic, header `len` == handle length, CRC over header+payload, footer.
   Any mismatch throws; otherwise return a `shared_ptr<const string>` of the payload.

## Crash recovery

- On open, scan from offset 0 for the last **framing-sound** record (magic ok, fits
  the file, header `len` == footer `len`).
- A **framing break** = torn tail from a crash mid-append → truncate from there.
- A record that is framing-sound but **CRC-fails** = interior bit-rot → left in place,
  stepped over, and rejected at `load()` — so one corrupt record does not discard the
  records written after it.

## User interface (public API)

```cpp
auto store = BlobStore::open({.dir = "/path"});         // Options: durability, cache, cap
Index i        = store->store(bytes, n);                // or store(string_view)
vector<Index> v = store->appendBatch({sv1, sv2, ...});  // one barrier for the batch
shared_ptr<const string> b   = store->load(i);          // throws on bad/corrupt handle
vector<shared_ptr<const string>> bs = store->loadBatch({i1, i2, ...});
store->sync();                                          // force durability
Stats s = store->stats();                               // counters
array<uint8_t,16> h = i.bytes();  Index::from_bytes(h); // persist/restore a handle
```

## Class layout

- **`Index`** — 16-byte handle `{segment:u32, length:u32, offset:u64}`; `valid()`,
  `bytes()`/`from_bytes()` for serialization.
- **`BlobStore`** — public facade (`open`, `store`, `appendBatch`, `load`, `loadBatch`,
  `sync`, `stats`); holds a `pImpl`.
- **`BlobStore::Impl`** — owns the segment fd + `cursor` (under `append_mu`), the
  group-commit syncer (`pending` waiters, `commit_mu/cv`, thread), the cache, and
  atomic stat counters. Contains framing, recovery, reserve, and read helpers.
- **`ShardedLruCache`** — N shards, each a `mutex` + LRU list + map, plus an in-flight
  map for single-flight. `get_or_load(key, loader, Outcome*)`.
- **`crc32c`** — table-driven Castagnoli CRC with incremental `crc` seed.

## Design decisions

- **Single segment, no rotation.** Simplest correct design; one fd, no registry. Cost:
  <4 GiB per store and `store()` throws when full.
- **Length kept in the handle.** Redundant with the on-disk header, but it lets a load
  size the read for **one `pread`** and gives an independent length witness.
- **CRC covers the header.** Closes the gap where a flipped `len` could mis-size a read;
  also makes offset-only indexing safe if ever wanted.
- **Group-commit default.** Trades a few ms of crash window for far higher write
  throughput; `sync` and `osbuffered` modes available per workload.
- **Sharded LRU + single-flight.** Buffered I/O means the OS page cache already holds
  recently-touched bytes, so the LRU is *not* there to keep data in RAM — it caches
  **work**: a hit skips the `pread` syscall, the CRC pass, and the payload copy, and
  returns a shared immutable buffer. It also coalesces the application's duplicate
  concurrent loads (single-flight), which the page cache cannot do. Cost: hot blobs are
  held twice (page cache + LRU).

## I/O model

- Reads and writes are **buffered** (no `O_DIRECT`): both go through the OS page cache.
  Writes are made durable by `fsync` per the durability mode.
- The LRU sits **above** the page cache. To get honest *device* read numbers the
  benchmark calls `evict_os_cache()` (`fsync` + `posix_fadvise(DONTNEED)`) before each
  cold read phase, so the LRU is the only cache and misses actually reach the disk.

## Performance (estimated from the design)

- **Reads:** one `pread` + one CRC pass. Small blobs are syscall-bound; large blobs are
  CPU-bound on software CRC + a payload copy (≈ a few hundred MB/s/core; HW CRC + a
  zero-copy path would lift this). A cache hit skips all of it.
- **Writes:** one `pwrite` + amortized `fsync`. Throughput scales with concurrency since
  only the tiny offset reservation is serialized; the `fsync` cost is shared by the batch.
- **Batch API:** one lock acquisition, one `pwrite`, one durability barrier for N blobs →
  tighter tail latency and far fewer `fsync`s than N single calls.
- **Cache:** repeated loads of a hot set serve from the LRU (no syscall/CRC/copy);
  concurrent duplicates coalesce to a single underlying read. For 1 MiB blobs the default
  64 MiB cache holds few entries, so a wide working set churns — size it to the workload.

## Security & permissions (within threat model)

- **Threat model:** accidental corruption (torn writes, bit-rot) and buggy callers — **not**
  a malicious party with write access to the `.seg` file.
- **Integrity:** CRC32C detects random corruption. It is **not** cryptographic — anyone who
  can edit the file can forge a matching CRC; tamper-evidence would need a keyed MAC.
- **Input safety:** a bad/garbage handle is bounds-checked and rejected (throw), not trusted;
  reads never run past EOF.
- **Permissions:** segment files created `0644`; access control is delegated to the host
  filesystem and directory permissions. Single-writer assumption is the caller's to enforce.

## Tests for the design (plain English)

**Unit** (isolated, deterministic)
- CRC32C matches known check vectors, is incremental, and catches bit flips.
- `Index` round-trips through 16-byte serialization; the default handle is invalid.
- Cache evicts by capacity and, under many concurrent duplicate loads, runs the loader once.
- A blob over the segment cap makes `store()` throw; `evict_os_cache()` drops pages without
  affecting correctness; `AsyncFlush` background-fsyncs on the size threshold.

**Integration** (full store, on disk)
- Store→load round-trips for empty, tiny, 4K, 1M, and odd sizes.
- Handles survive close/reopen (opaque handle is self-describing).
- `appendBatch` returns one loadable handle per blob; `loadBatch` returns values in order
  and survives reopen.
- Recovery: a torn tail is truncated while prior records load and new writes work; an
  interior bit-flip and a flipped length field are caught at load while neighbours (before
  **and after**) survive.

**Stress / fuzz** (deterministic seeds, so failures reproduce)
- Thousands of random-sized stores with immediate readback and post-reopen checks.
- Many threads storing/loading concurrently observe no torn or wrong bytes; all durable
  after reopen.
- Random on-disk faults (trailing garbage, bit flips, truncation): load is always
  correct-or-throws, and pure trailing garbage loses nothing.
- Mutation check: disabling the load-time CRC must make the corruption tests fail (proves
  the tests have teeth).

**Performance**
- Benchmark matrix per durability mode: sequential/concurrent write, batch write, and
  sequential/random/cached read at 4K and 1M, reporting MB/s, op/s, p50/p99, and the
  write-phase `fsync` count. Each cold read phase evicts the page cache first so reads hit
  the device, not RAM.

**Report**
- Each suite prints pass/fail per case; CI runs `ctest`. Bench prints the matrix so
  regressions are visible across durability modes and changes.

### Is the unit suite enough?

For the **functional contract and the in-scope failure points, yes** — round-trips, framing,
recovery, cache/single-flight, batch, and the durability knobs are covered, and the fuzzer +
mutation test guard against vacuous passing. Known **gaps** to be honest about:
- **No true power-loss test.** Faults are injected by closing the store and mutating files,
  which models torn writes and bit-rot but **not** loss of un-`fsync`'d data on a real crash;
  that needs process-kill or an `fsync`-failure injection harness (out of current scope).
- **Durability modes aren't crash-matrixed.** `GroupCommit`/`Sync` are exercised for
  correctness but not for "returned write survives an unclean exit" across all four modes.
- **Performance is not a unit test** — it lives in the bench and is read by a human; there is
  no automated perf-regression gate.
