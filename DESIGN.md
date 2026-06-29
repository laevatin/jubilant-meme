# recordstore — Design Document

A C++17 store for arbitrary-length binary records: write a record, get an opaque
**index**; load it back by that index. Built for concurrent loads/stores and crash
recovery. It is split into two decoupled halves that share **no in-memory state** —
a `RecordWriter` (append/durability/recovery) and a `RecordReader` (load/scan) — wired
by a small `main` driver. They communicate only through the file on disk and the
opaque handle.

## Assumptions

- Workload is **write-once, read-many**; no in-place update or delete.
- Blobs are **immutable** once stored; a handle stays valid for the store's life.
- Hot block sizes are **4 KiB and 1 MiB**, but any size up to ~4 GiB is accepted.
- A single store lives on **one local POSIX filesystem**; `pwrite`/`pread`/`fsync`
  behave per POSIX. One process opens a given store directory at a time.
- A writer and any number of readers may run concurrently on the same store.
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
   RecordWriter                              RecordReader
   append()/appendBatch()                    load()/loadBatch()/scan()
        |                                          |
   frame + reserve offset                     ReadCache (no-op placeholder)
   (append_mu)                                     |
        |                                     pread + verify CRC/framing
   pwrite (lockless, positioned)                   |
        |                                     std::string (by value)
   durability barrier <-- syncer thread
   (group-commit / sync / async / osbuffered)
        |
   one append-only segment file (000001.seg)  <-- shared only on disk
```

- **Writer and reader are separate objects** with their own file descriptors and no
  shared memory; the writer owns the append cursor, the reader is read-only.
- **Append path:** frame the record(s), reserve a byte range under a tiny mutex,
  `pwrite` outside the lock, then wait for the durability barrier. The whole append
  holds a **shared** lifetime lock; `close()` takes it **exclusive**, so it waits for
  in-flight appends and blocks new ones before closing the fd — a `pwrite`/`fsync`
  never races a `close()` (which would otherwise hit a closed or recycled fd).
- **Syncer thread:** for group-commit, coalesces queued writers and issues one
  `fsync` per batch, then wakes them; for async-flush, fsyncs on a size/time threshold.
- **Read path:** route through `ReadCache` (currently a pass-through that always misses),
  do one positioned `pread`, validate, and return a copy of the payload.

## File format

### Directory & segment layout

A store *is* a directory. It holds exactly one segment file:

```
<dir>/
  └── 000001.seg     # the whole log; created 0644 on first open
```

- **One file, one log.** All records live in `000001.seg`; there is no separate index,
  manifest, or write-ahead file. The segment id is fixed at `1` (`%06u.seg` naming is
  kept so the on-disk layout is forward-compatible if rotation is ever added).
- **The file is the index.** A handle `(segment=1, length, offset)` points straight at a
  record's header byte, so nothing else needs to be persisted or rebuilt on open.
- **No header/superblock.** The file begins immediately with the first record. Its length
  is the durable cursor: valid data is `[0, last_sound_record_end)`; anything past that is
  a torn tail. Capacity is bounded by `segment_size` (< 4 GiB).
- **Append-only & position-addressed.** Records are written once at increasing offsets and
  never moved or rewritten, so every offset ever returned stays valid for the file's life.

### Record framing

Records are packed back to back, each framed as:

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
- Little-endian on disk. A record occupies `12 + len + 4` bytes; the next record begins
  immediately after the footer.
- The header + footer make the stream **self-delimiting in both directions of trust**:
  `magic` marks a record start, `len` sizes it, and `footer == len` confirms the record
  ended where the header claimed — the basis for recovery below.

## Write procedure (`RecordWriter::append` / `appendBatch`)

1. Frame the record into a buffer (`magic | len | crc | payload | footer`).
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

## Read procedure (`RecordReader::load` / `loadBatch` / `scan`)

1. Route the load through `ReadCache::get_or_load`. Today it is a no-op placeholder that
   always invokes the loader (no caching), so every load goes to disk; the seam is kept so
   a real LRU + single-flight can return without changing the interface.
2. `pread` `12 + len + 4` bytes at `offset` (the handle carries `len` → one syscall).
3. Verify: magic, header `len` == handle length, CRC over header+payload, footer.
   Any mismatch throws; otherwise return a **`std::string` by value** (a copy of the
   payload — the zero-copy `shared_ptr<const string>` path was removed for now).
4. `scan()` snapshots the file size and walks the framing from offset 0, yielding each
   record (CRC-verified) by value; a torn tail / framing break ends the scan, a
   framing-sound CRC-bad record is skipped.

## Opening a (possibly corrupted) file

`open()` must turn an arbitrary on-disk file — including one left by a crash mid-append or
damaged by bit-rot — into a consistent store with a correct write cursor. It never trusts
the file blindly; it re-derives the durable boundary by walking the framing.

**Recovery scan.** Starting at offset 0, for each record read the 12-byte header and the
4-byte footer and classify it:

1. **Framing-sound** — `magic == 'BLOB'`, the record `[off, off+12+len+4)` fits within the
   file, and `footer == len`. Accept the frame, advance `cursor` past it, continue.
2. **Framing break** — `magic` wrong, the record would run past EOF, or `footer != len`.
   Stop here: everything from this offset on is a **torn tail** from a crash partway
   through an append (or trailing garbage). `ftruncate` the file to this offset and set
   `cursor` to it. The next append starts cleanly at the boundary.
3. **CRC mismatch but framing-sound** — the frame is well-formed but `crc32c` over
   header+payload disagrees: **interior bit-rot** in an otherwise intact record. The record
   is *kept in place* and stepped over (its length is trustworthy because the framing is
   sound and CRC-checked), so records written *after* it are not discarded. It is rejected
   later, at `load()`/`scan()` time, never returned as wrong bytes.

The split between cases 2 and 3 is the key invariant: **a torn tail truncates, interior
corruption is contained.** One bad record in the middle costs only that record, not the
suffix of the log.

**Edge cases handled on open:**
- **Missing file / empty directory** — create `000001.seg`, `cursor = 0`, empty store.
- **Zero-length file** — valid empty store; first append writes at offset 0.
- **Corruption at offset 0** (bad magic in the very first header) — treated as a framing
  break at 0: the file truncates to empty rather than mis-parsing garbage as a record.
- **Partial header/footer at EOF** — fewer than `12 + len + 4` bytes available is a framing
  break → truncated.
- The recovery read goes through the page cache like any other read; `evict_os_cache()`
  afterward gives benchmarks an honest cold start.

**What recovery does *not* do:** it has no separate journal to replay and cannot recover
data that was never `fsync`'d before a crash (see the durability modes for each mode's loss
window). It restores *structural* consistency and a correct cursor; it does not invent lost
writes.

## Behavior under kernel panic / machine failure

A **process crash** keeps the OS page cache, so anything `pwrite` reached survives
regardless of `fsync`. A **kernel panic or power loss** is the real test: the page cache is
volatile and gone — only bytes the device actually persisted (and acknowledged via a flush)
survive. (We do not test this directly; this is the reasoned analysis.)

**What's volatile vs durable.** Volatile and lost: the page cache, the in-memory `cursor`,
the group-commit queue, the syncer thread. Durable: only the bytes in
`000001.seg` that were `fsync`'d *and* flushed by the device. Crucially, **no in-memory
state needs to survive** — the cursor and the entire index are re-derived by the recovery
scan, and the handle is self-describing. There is no separate index/manifest file that
could be persisted out of order against the data, so an whole class of crash-consistency
bugs is absent by construction.

**Two physical hazards a panic adds:**
- **Torn writes.** A `pwrite` is not atomic across a power cut. Sector writes are atomic at
  the device, but a multi-sector record can persist partially, and the page cache may write
  pages back **out of order** — an un-`fsync`'d region can hold an arbitrary subset of its
  bytes.
- **Size-vs-data ordering.** After a panic the file may be shorter than what we wrote, or
  (on non-CoW filesystems) the extended region may expose **stale neighbor block contents**.

Both are caught by the framing: `magic` + `len` + `footer == len` + `crc32c` reject a short
file, stale/zero bytes, or a half-written record. On **btrfs** (our target) CoW means an
append never overwrites committed records in place, data is independently checksummed, and a
panic rolls back to the last committed transaction rather than leaving structural garbage.

**Per durability mode (does an *acked* write survive a panic?):**

| Mode         | Acked write survives? | Loss window on panic |
|--------------|-----------------------|----------------------|
| `Sync`       | **Yes** — `fsync` completes before `store()` returns | only in-flight appends the caller was never told succeeded |
| `GroupCommit`| **Yes** — blocks until the batched `fsync` covering it completes | same as `Sync`; a writer still parked on the syncer hasn't returned |
| `AsyncFlush` | **No (bounded)** — returns before `fsync` | up to `async_flush_bytes` **or** `async_flush_interval_us` of acked writes |
| `OsBuffered` | **No** — durable only at `sync()`/`close()` | everything since the last explicit `sync()` |

`Sync` and `GroupCommit` lose **nothing the caller was told succeeded** — that is the point
of GroupCommit returning durable-before-return (Sync's panic guarantee at a fraction of the
`fsync` count). `AsyncFlush`/`OsBuffered` trade a bounded, documented window of acked writes
for throughput.

**Recovery resolves every post-panic file state** into a consistent monotonic prefix: a
framing-sound prefix is accepted; the first framing break (torn tail / stale garbage) is
truncated; recovery **stops at the first gap**, so out-of-order persistence can never expose
a record after a hole. In all modes the store reopens consistent, never returns wrong bytes,
and never faults on open — modes differ only in *how long* the surviving prefix is.

**Known rough edge.** Recovery checks **framing only**, not CRC, so it keeps a
framing-sound-but-CRC-bad record in place (deliberate, to contain interior bit-rot). If a
panic tears the *tail* such that the 12-byte header and 4-byte footer persist intact but the
payload is half-stale, that torn record is framing-sound-but-CRC-bad: recovery **keeps** it
(it becomes a permanent interior-corrupt record `load()` will always reject) and **continues
past it**, possibly **resurrecting** a later record whose bytes also reached disk. This
violates no contract — those records were never acked, and `load` still fails safe (throws,
never wrong bytes) — but a clean "truncate the torn tail" degrades into "a stuck corrupt
record + a resurrected straggler." Probability is low (header *and* footer must survive while
the CRC'd middle does not). A fix would treat a CRC-bad record with nothing sound after it as
a torn tail and truncate; recovery currently cannot distinguish interior rot from a
sound-looking torn tail.

**Caveats below our assumptions:** the guarantee is only as strong as `fsync` → device
flush — a drive without power-loss protection that acks a flush but loses its volatile cache
can still tear/lose "durable" data. Linux `fsync`-error semantics ("fsyncgate") can also lose
data silently after a failed `fsync`; we account `fsync` calls but do not re-architect around
`fsync` returning `EIO`.

## User interface (public API)

```cpp
// --- write side ---
auto w = RecordWriter::open({.dir = "/path"});          // Options: durability, cap
Index i         = w->append(value);                     // value: const std::string&
vector<Index> v = w->appendBatch({v1, v2, ...});        // one barrier for the batch
w->sync();                                              // force durability
w->close();                                             // flush + stop syncer + close fd
RecordWriter::Stats ws = w->stats();                    // appends, bytes, fsyncs

// --- read side (separate object, opens the same dir read-only) ---
auto r = RecordReader::open({.dir = "/path"});
std::string b              = r->load(i);                // throws on bad/corrupt handle
vector<std::string> bs     = r->loadBatch({i1, i2, ...});
for (auto& rec : r->scan()) { rec.index; rec.value; }   // forward scan, all records
RecordReader::Stats rs = r->stats();                    // loads

array<uint8_t,16> h = i.bytes();  Index::from_bytes(h); // persist/restore a handle
```

## Class layout

- **`Index`** — 16-byte handle `{segment:u32, length:u32, offset:u64}`; `valid()`,
  `bytes()`/`from_bytes()` for serialization (header-only).
- **`RecordWriter`** — `open`, `append`, `appendBatch`, `sync`, `close`, `stats`; holds a
  `pImpl`. Its `Impl` owns the segment fd + `cursor` (under `append_mu`), the group-commit /
  async syncer (`pending` waiters, `commit_mu/cv`, thread), recovery, and atomic counters.
- **`RecordReader`** — `open`, `load`, `loadBatch`, `scan`, `stats`; holds a read-only fd and
  a `ReadCache`. `scan()` returns a forward `Iterator` range (input iterator) that walks the
  framing from offset 0, verifies CRC, skips a CRC-bad record, and stops at a torn tail.
- **`ReadCache`** — placeholder pass-through (`get_or_load` always misses); the seam for a
  future LRU + single-flight.
- **`fmt` (src/record_format.h)** — internal: constants, byte helpers, `pread/pwrite_all`,
  `segment_path`, `frame_record`. Shared by both `.cpp`s; not part of the public API.
- **`crc32c`** — Castagnoli CRC with incremental `crc` seed. Uses the x86 SSE4.2 `crc32`
  instruction when the CPU advertises it (runtime-detected once), with a table-driven
  software fallback; both paths give identical values.

## Design decisions

- **Writer / reader split, no shared memory.** The two halves communicate only through the
  file and the opaque handle, so a reader needs no writer state and there is no index/manifest
  to keep in sync. Recovery rederives the cursor; the handle is self-describing.
- **Single segment, no rotation.** Simplest correct design; one fd, no registry. Cost:
  <4 GiB per store and `append()` throws when full.
- **Length kept in the handle.** Redundant with the on-disk header, but it lets a load
  size the read for **one `pread`** and gives an independent length witness.
- **CRC covers the header.** Closes the gap where a flipped `len` could mis-size a read;
  also makes offset-only indexing safe if ever wanted.
- **Group-commit default.** Trades a few ms of crash window for far higher write
  throughput; `sync` and `osbuffered` modes available per workload.
- **Reads by value, cache removed (for now).** `load()` returns `std::string` by value and
  the cache is a no-op placeholder. This keeps the read path trivially correct and the writer/
  reader split clean while the LRU + single-flight + zero-copy shared buffers are re-designed
  behind the unchanged `ReadCache` seam. Cost: every load copies the payload and pays the
  syscall + CRC; duplicate concurrent loads are not yet coalesced.

## I/O model

- Reads and writes are **buffered** (no `O_DIRECT`): both go through the OS page cache.
  Writes are made durable by `fsync` per the durability mode.
- With the cache disabled, the OS page cache is the only read cache. For honest *device*
  read numbers a reader calls `evict_os_cache()` (`posix_fadvise(DONTNEED)`) before a cold
  read phase so misses actually reach the disk.

## Performance (estimated from the design)

- **Reads:** one `pread` + one CRC pass + one payload copy out. Small records are
  syscall-bound; large records are CPU-bound on the CRC + the copy. CRC uses the SSE4.2
  `crc32` instruction (multiple GB/s/core), so for large records the payload copy now
  dominates; a zero-copy path would lift it further. With no cache, repeated loads of a hot
  record re-pay this each time (served from the page cache, so no device I/O).
- **Writes:** one `pwrite` + amortized `fsync`. Throughput scales with concurrency since
  only the tiny offset reservation is serialized; the `fsync` cost is shared by the batch.
- **Batch API:** one lock acquisition, one `pwrite`, one durability barrier for N records →
  tighter tail latency and far fewer `fsync`s than N single calls.
- **Cache:** a future re-add would let repeated loads of a hot set skip syscall/CRC/copy and
  coalesce duplicate concurrent loads, sized to the workload — not present today.

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

**Unit / functional** (`test_crc32c`, `test_writer_reader`)
- CRC32C matches known check vectors, is incremental, and catches bit flips.
- `Index` round-trips through 16-byte serialization; the default handle is invalid.
- `append`→`load` round-trips for empty, tiny, 4K, 1M, and odd sizes; distinct appends get
  distinct handles/values; the writer counts appends, the reader counts loads.
- A record over the segment cap makes `append()` throw; `append` after `close()` throws;
  `evict_os_cache()` drops pages without affecting correctness; `AsyncFlush` background-fsyncs
  on the size threshold and the data survives reopen.

**Integration** (full writer + reader, on disk — `test_writer_reader`)
- Handles survive writer close → reader reopen (opaque handle is self-describing).
- `appendBatch` returns one loadable handle per record; `loadBatch` returns values in order
  and survives reopen.
- The forward `scan()` yields every record in write order (empty store → nothing, works after
  reopen) and skips a CRC-corrupt record while yielding its neighbours.

**Integration — recovery** (`test_recovery`)
- A torn tail is truncated by the writer on open while prior records load and new writes work;
  an interior bit-flip and a flipped length field are caught at load while neighbours (before
  **and after**) survive; a framing-sound CRC-bad interior record is *not* truncated (file
  size unchanged). Partial header / truncated payload at the tail truncate exactly to the last
  good record. Empty/missing dir yields a usable store; `OsBuffered` survives an explicit sync.

**Integration — thread safety** (`test_concurrency`, run under TSan/ASan)
These gate the concurrent-`append` / concurrent-`load`+`scan` claims; each asserts *no torn or
wrong bytes* and (where applicable) durability after reopen. **Implemented** — and TSan caught
a real stack-`Waiter` condition-variable use-after-free in group commit, now fixed.
- **Concurrent writers, disjoint ranges.** N threads each append M records; assert N·M
  handles with **distinct, non-overlapping** byte ranges (the `[cursor, cursor+total)`
  reservation never double-allocates), and every handle loads back exactly.
- **Concurrent writers + readers (read-your-writes across threads).** While writers append and
  publish handles to a shared list, reader threads load random published handles; every load
  returns the exact bytes that writer stored.
- **Live `scan()` during writes.** A scan run concurrently with a writer only ever yields
  complete, CRC-valid records matching what was written; the record count is monotonic and the
  final scan sees all records.
- **Interleaved `append` + `appendBatch`.** Some threads append singly, others in batches; all
  handles stay disjoint and load back.
- **Durability under concurrency.** Concurrent `GroupCommit` appends all survive a clean close
  + reopen (every acked write present).
- *(Single-flight / cache-eviction races are out of scope while the cache is a no-op
  placeholder; they return when the LRU does.)*

**Stress / fuzz** (`test_fuzz`, deterministic seeds so failures reproduce)
- Thousands of random-sized appends with immediate readback and post-reopen checks.
- Many threads appending/loading concurrently observe no torn or wrong bytes; reserved offsets
  are disjoint; all durable after reopen.
- Random on-disk faults (trailing garbage, bit flips, truncation): load is always
  correct-or-throws, and pure trailing garbage loses nothing.

**Performance** (`bench`)
- Benchmark matrix per durability mode: sequential/concurrent append, batch append, and
  sequential/random read at 4K and 1M, reporting MB/s, op/s, p50/p99, and the write-phase
  `fsync` count. Each cold read phase evicts the page cache first so reads hit the device.

**Report**
- Each suite prints pass/fail per case; CI runs `ctest`. Bench prints the matrix so
  regressions are visible across durability modes and changes.

### Is the suite enough?

For the **functional contract and the in-scope failure points, yes** — round-trips, framing,
recovery, batch, the durability knobs, and concurrency (writer/reader/scan, TSan-clean) are
covered, and the fuzzer guards against vacuous passing. Known **gaps** to be honest about:
- **No true power-loss test.** Faults are injected by closing the store and mutating files,
  which models torn writes and bit-rot but **not** loss of un-`fsync`'d data on a real crash;
  that needs process-kill or an `fsync`-failure injection harness (out of current scope).
- **Durability modes aren't crash-matrixed.** `GroupCommit`/`Sync` are exercised for
  correctness but not for "returned write survives an unclean exit" across all four modes.
- **Performance is not a unit test** — it lives in the bench and is read by a human; there is
  no automated perf-regression gate.
