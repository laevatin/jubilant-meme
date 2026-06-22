// blob_store.h — public API contract for the log-structured blob store.
//
// Write an arbitrary-length binary blob -> get an opaque Index. Load by Index.
// Concurrent stores/loads, group-commit durability, crash recovery, read cache.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

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
    std::array<uint8_t, 16> bytes() const;
    static Index from_bytes(const std::array<uint8_t, 16>& b);
};

enum class Durability {
    GroupCommit,  // batched fsync (default) — may lose last few ms on crash
    Sync,         // fsync before every store() returns — zero loss, slow
    OsBuffered,   // never fsync except on close()/sync() — fastest, least safe
};

class BlobStore {
public:
    struct Options {
        std::string dir;                       // directory holding the segment file (created if absent)
        uint64_t segment_size      = 256ull << 20;  // hard capacity cap; store() throws when full (must be < 4 GiB)
        Durability durability      = Durability::GroupCommit;
        uint64_t   group_commit_interval_us = 1000;  // syncer fsync cadence
        size_t     group_commit_max_batch   = 1024;  // early-flush when this many waiters queue
        bool       enable_cache    = true;
        size_t     cache_capacity_bytes = 64ull << 20;
        size_t     cache_shards         = 16;
    };

    struct Stats {
        uint64_t stores = 0;
        uint64_t loads = 0;
        uint64_t bytes_written = 0;
        uint64_t cache_hits = 0;
        uint64_t cache_misses = 0;
        uint64_t coalesced = 0;   // duplicate concurrent loads collapsed by single-flight
        uint64_t fsyncs = 0;
        uint32_t segments = 0;
    };

    static std::unique_ptr<BlobStore> open(const Options& opts);
    ~BlobStore();

    // Store a blob. Returns its Index. Blocks until durable per the Durability mode.
    // Thread-safe; may be called concurrently from many threads.
    Index store(const void* data, size_t len);
    Index store(std::string_view sv) { return store(sv.data(), sv.size()); }

    // Store many blobs as one append: a single offset reservation, a single
    // contiguous write, and a single durability barrier amortized across the whole
    // batch. Returns one Index per blob, in the same order. Thread-safe.
    std::vector<Index> appendBatch(const std::vector<std::string_view>& blobs);

    // Load a blob by Index. Returns shared, immutable bytes (cache-friendly).
    // Throws std::runtime_error on a corrupt/out-of-range handle. Thread-safe.
    std::shared_ptr<const std::string> load(Index idx);

    // Load many blobs by Index. Returns one shared buffer per Index, in order;
    // each goes through the read cache / single-flight exactly like load().
    // If any handle is bad the whole call throws. Thread-safe.
    std::vector<std::shared_ptr<const std::string>> loadBatch(const std::vector<Index>& idxs);

    // Force all buffered writes durable (fsync). No-op data loss window afterward.
    void sync();

    Stats stats() const;

    BlobStore(const BlobStore&) = delete;
    BlobStore& operator=(const BlobStore&) = delete;

private:
    BlobStore();
    struct Impl;
    std::unique_ptr<Impl> p_;
};

}  // namespace bs
