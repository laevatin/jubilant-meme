// record_writer.h — the append/durability half of the store.
//
// Owns the single append-only segment file, the write cursor, and the durability
// machinery (inline/group-commit/async fsync). On open it runs crash recovery,
// truncating any torn tail and rederiving the cursor. Holds no reader state.
#pragma once

#include "record.h"

#include <memory>
#include <string>
#include <vector>

namespace bs {

class RecordWriter {
public:
    struct Stats {
        uint64_t appends       = 0;
        uint64_t bytes_written = 0;  // payload bytes (excludes framing)
        uint64_t fsyncs        = 0;
        uint32_t segments      = 1;
    };

    // Open (creating the directory/segment if absent) and recover any torn tail.
    static std::unique_ptr<RecordWriter> open(const Options& opts);
    ~RecordWriter();

    // Append one record. Returns its Index. Blocks until durable per Durability.
    // Thread-safe; may be called concurrently from many threads.
    Index append(const std::string& value);

    // Append many records as one physical append: a single offset reservation, a
    // single contiguous pwrite, and one durability barrier amortized across the
    // whole batch. Returns one Index per value, in order. Thread-safe.
    std::vector<Index> appendBatch(const std::vector<std::string>& values);

    // Force all buffered writes durable (fsync). Safe to call anytime.
    void sync();

    // Flush, stop the syncer, and close the segment fd. Waits for any in-flight
    // concurrent append()/sync() to finish before closing, and blocks new ones, so
    // the fd is never closed out from under an active writer. Idempotent; after
    // close() append() throws and sync() is a no-op. The destructor calls close().
    void close();

    Stats stats() const;

    RecordWriter(const RecordWriter&) = delete;
    RecordWriter& operator=(const RecordWriter&) = delete;

private:
    RecordWriter();
    struct Impl;
    std::unique_ptr<Impl> p_;
};

}  // namespace bs
