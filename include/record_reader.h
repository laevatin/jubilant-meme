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

    // ---- Forward scan -----------------------------------------------------
    // Walk every record in the segment, in write order, following the
    // length-delimited framing from offset 0. CRC is verified per record: a torn
    // tail / framing break ends the scan; a framing-sound but CRC-bad record is
    // skipped. The end is snapshotted at scan() time; records appended after are
    // not seen. Safe to run concurrently with a writer and with load().
    class Iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = Record;
        using difference_type = std::ptrdiff_t;
        using pointer = const Record*;
        using reference = const Record&;

        Iterator() = default;
        Iterator& operator++() { advance(); return *this; }
        reference operator*() const { return cur_; }
        pointer operator->() const { return &cur_; }
        bool operator==(const Iterator& o) const { return at_end_ && o.at_end_; }
        bool operator!=(const Iterator& o) const { return !(*this == o); }

    private:
        friend class RecordReader;
        void advance();
        RecordReader* owner_ = nullptr;
        uint64_t off_ = 0;
        uint64_t end_ = 0;
        bool at_end_ = true;
        Record cur_;
    };

    struct Scan {
        Iterator first;
        Iterator begin() const { return first; }
        Iterator end() const { return Iterator{}; }
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
