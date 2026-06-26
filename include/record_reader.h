// record_reader.h — the read half of the store.
//
// Opens the segment file read-only and serves records by Index (one positioned
// pread + CRC verify) or as a forward scan in write order. Holds no writer state:
// a reader and a writer share only the file on disk. Reads are returned by value
// (std::string) — the zero-copy shared-buffer path was removed for now.
#pragma once

#include "read_cache.h"
#include "record.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace bs {

// One record yielded by a scan: its handle and its bytes.
struct Record {
    Index       index;
    std::string value;
};

class RecordReader {
public:
    struct Stats {
        uint64_t loads = 0;
    };

    // Open the segment for reading. Does not modify the file (recovery/truncation
    // is the writer's job); a torn tail simply ends a scan early.
    static std::unique_ptr<RecordReader> open(const Options& opts);
    ~RecordReader();

    // Load a record by Index. Returns a copy of its bytes. Throws std::runtime_error
    // on a corrupt/out-of-range handle. Thread-safe.
    std::string load(Index idx);

    // Load many records by Index, in order. If any handle is bad the whole call
    // throws. Thread-safe.
    std::vector<std::string> loadBatch(const std::vector<Index>& idxs);

    // ---- Forward scan (C++20) ---------------------------------------------
    // Walk every record in the segment, in write order, following the
    // length-delimited framing from offset 0. CRC is verified per record: a torn
    // tail / framing break ends the scan; a framing-sound but CRC-bad record is
    // skipped. The end is snapshotted at scan() time; records appended after are
    // not seen. Safe to run concurrently with a writer and with load().
    //
    // A single-pass C++20 `std::input_iterator`: it advertises `iterator_concept`
    // (not the legacy `iterator_category`) and compares against a
    // `std::default_sentinel_t` end marker rather than a same-type end iterator.
    class Iterator {
    public:
        using iterator_concept = std::input_iterator_tag;
        using value_type = Record;
        using difference_type = std::ptrdiff_t;

        Iterator() = default;

        const Record& operator*() const { return cur_; }
        const Record* operator->() const { return &cur_; }
        Iterator& operator++() { advance(); return *this; }
        void operator++(int) { advance(); }  // single-pass: post-increment yields void

        // End is reached when the scan can advance no further; compared to the
        // sentinel, the rewritten !=/symmetric forms come for free in C++20.
        bool operator==(std::default_sentinel_t) const { return at_end_; }

    private:
        friend class RecordReader;
        void advance();
        RecordReader* owner_ = nullptr;
        uint64_t off_ = 0;
        uint64_t end_ = 0;
        bool at_end_ = true;
        Record cur_;
    };

    // An input range over the scan: begin() is the iterator, end() is the sentinel.
    struct Scan {
        Iterator first;
        Iterator begin() const { return first; }
        std::default_sentinel_t end() const { return std::default_sentinel; }
    };
    Scan scan();

    // Drop the segment's pages from the OS page cache (fsync + posix_fadvise
    // DONTNEED) so subsequent reads hit the device — used for honest cold-read
    // benchmarks now that reads are buffered. Best-effort.
    void evict_os_cache();

    Stats stats() const;

    RecordReader(const RecordReader&) = delete;
    RecordReader& operator=(const RecordReader&) = delete;

private:
    RecordReader();
    struct Impl;
    // One step of a forward scan: read the record at `off` (< `end`), fill `out`,
    // advance `off` past it. Returns false at end / torn tail. Used by Iterator.
    bool iter_step(uint64_t& off, uint64_t end, Record& out);
    std::unique_ptr<Impl> p_;
};

}  // namespace bs
