// record.h — shared public types for the log-structured record store.
//
// The store is split into two decoupled halves that share NO in-memory state:
//   - RecordWriter (record_writer.h): owns the append path, cursor, durability.
//   - RecordReader (record_reader.h): opens the same file read-only and loads
//     records by Index, or scans them in write order.
// The only thing they share is the file on disk and the opaque Index handle below.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace bs {

// Opaque packed handle. 16 bytes: (segment, length, offset).
// This store keeps a single segment (file), so segment is always 1; the field is
// retained so the on-disk handle format is stable. segment == 0 is the invalid
// sentinel.
struct Index {
    uint32_t segment = 0;  // 0 => invalid; always 1 for a live handle
    uint32_t length  = 0;  // payload length in bytes
    uint64_t offset  = 0;  // byte offset of the record header within the segment

    bool valid() const { return segment != 0; }
    bool operator==(const Index& o) const {
        return segment == o.segment && length == o.length && offset == o.offset;
    }
    bool operator!=(const Index& o) const { return !(*this == o); }

    // Serialize/deserialize so callers can persist a handle (little-endian).
    std::array<uint8_t, 16> bytes() const {
        std::array<uint8_t, 16> b{};
        auto w = [&](size_t at, uint32_t v) {
            b[at] = uint8_t(v); b[at + 1] = uint8_t(v >> 8);
            b[at + 2] = uint8_t(v >> 16); b[at + 3] = uint8_t(v >> 24);
        };
        w(0, segment);
        w(4, length);
        w(8, uint32_t(offset & 0xFFFFFFFF));
        w(12, uint32_t(offset >> 32));
        return b;
    }
    static Index from_bytes(const std::array<uint8_t, 16>& b) {
        auto r = [&](size_t at) -> uint32_t {
            return uint32_t(b[at]) | (uint32_t(b[at + 1]) << 8) |
                   (uint32_t(b[at + 2]) << 16) | (uint32_t(b[at + 3]) << 24);
        };
        Index i;
        i.segment = r(0);
        i.length = r(4);
        i.offset = uint64_t(r(8)) | (uint64_t(r(12)) << 32);
        return i;
    }
};

enum class Durability {
    GroupCommit,  // batched fsync (default); append() blocks until durable — may lose last few ms on crash
    Sync,         // fsync before every append() returns — zero loss, slow
    AsyncFlush,   // append() returns after pwrite; a background worker fsyncs once unsynced
                  // bytes exceed async_flush_bytes OR async_flush_interval_us elapses.
                  // Lowest write latency; a crash loses at most that window of acked writes.
    OsBuffered,   // never fsync except on close()/sync() — fastest, least safe
};

// Configuration shared by writer and reader (each uses the fields it needs).
struct Options {
    std::string dir;                            // directory holding the segment file (created if absent)

    // ---- writer ----
    uint64_t   segment_size            = 256ull << 20;  // hard capacity cap; append() throws when full (< 4 GiB)
    Durability durability              = Durability::GroupCommit;
    uint64_t   group_commit_interval_us = 1000;         // syncer fsync cadence
    size_t     group_commit_max_batch   = 1024;         // early-flush when this many waiters queue
    uint64_t   async_flush_bytes        = 4ull << 20;   // AsyncFlush: fsync once this many unsynced bytes accumulate
    uint64_t   async_flush_interval_us  = 2000;         // AsyncFlush: ...or this long since the last flush

    // ---- reader ----
    // The read cache is currently a no-op placeholder (see read_cache.h); these
    // are retained so the LRU can be reintroduced without an interface change.
    bool   enable_cache         = false;
    size_t cache_capacity_bytes = 64ull << 20;
    size_t cache_shards         = 16;
};

}  // namespace bs
