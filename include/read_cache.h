// read_cache.h — placeholder for the reader's record cache.
//
// The earlier design had a sharded LRU + single-flight here. It has been removed
// for now so the read path is dead simple: every load goes straight to disk and
// is CRC-verified. This struct keeps the *seam* — RecordReader routes loads
// through get_or_load() — so a real cache (LRU, single-flight, zero-copy shared
// buffers) can be reintroduced later without touching RecordReader's interface.
#pragma once

#include "record.h"

#include <string>

namespace bs {

// No-op pass-through: never caches, always calls the loader. Thread-safe by
// virtue of holding no state.
struct ReadCache {
    explicit ReadCache(const Options& = {}) {}

    // Mirrors the shape of a real cache: look up `idx`, and on a miss invoke
    // `load()` to fetch it. Today it is always a miss.
    template <class Loader>
    std::string get_or_load(const Index& /*idx*/, Loader&& load) {
        return load();
    }
};

}  // namespace bs
