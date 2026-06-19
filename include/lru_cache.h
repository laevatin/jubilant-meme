// lru_cache.h — sharded LRU with single-flight load coalescing.
//
// Keyed by uint64. Values are shared_ptr<const string> so the cache and all
// callers share one immutable buffer. When several threads miss on the same key
// at once, exactly one runs the loader; the rest block and receive its result
// (single-flight) — this is what collapses the app layer's duplicate loads.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace bs {

class ShardedLruCache {
public:
    using ValuePtr = std::shared_ptr<const std::string>;

    // Per-call telemetry, accumulated by the caller.
    struct Outcome {
        bool hit = false;        // served from resident LRU
        bool coalesced = false;  // waited on another thread's in-flight load
    };

    ShardedLruCache(size_t capacity_bytes, size_t shards)
        : shards_(std::max<size_t>(1, shards)),
          per_shard_cap_(std::max<size_t>(1, capacity_bytes / std::max<size_t>(1, shards))),
          tab_(shards_) {}

    // Return the cached value for `key`, or run `loader()` exactly once across
    // concurrent callers, cache it, and return it.
    ValuePtr get_or_load(uint64_t key,
                         const std::function<ValuePtr()>& loader,
                         Outcome* out = nullptr) {
        Shard& s = tab_[key % shards_];
        std::unique_lock<std::mutex> lk(s.mu);

        if (auto it = s.map.find(key); it != s.map.end()) {
            s.lru.splice(s.lru.begin(), s.lru, it->second);  // touch (MRU)
            if (out) out->hit = true;
            return it->second->second;
        }

        // Single-flight: is someone already loading this key?
        if (auto fit = s.inflight.find(key); fit != s.inflight.end()) {
            auto fl = fit->second;
            lk.unlock();
            if (out) out->coalesced = true;
            return fl->wait();
        }

        auto fl = std::make_shared<InFlight>();
        s.inflight.emplace(key, fl);
        lk.unlock();

        ValuePtr val;
        try {
            val = loader();
        } catch (...) {
            lk.lock();
            s.inflight.erase(key);
            lk.unlock();
            fl->fail();  // wake waiters so they can retry/observe failure
            throw;
        }

        lk.lock();
        s.inflight.erase(key);
        insert_locked(s, key, val);
        lk.unlock();
        fl->set(val);
        return val;
    }

    void put(uint64_t key, ValuePtr val) {
        Shard& s = tab_[key % shards_];
        std::lock_guard<std::mutex> lk(s.mu);
        insert_locked(s, key, val);
    }

    // Test/inspection helper: present in the resident set?
    bool contains(uint64_t key) const {
        const Shard& s = tab_[key % shards_];
        std::lock_guard<std::mutex> lk(s.mu);
        return s.map.count(key) != 0;
    }

    size_t size_bytes() const {
        size_t total = 0;
        for (const auto& s : tab_) {
            std::lock_guard<std::mutex> lk(s.mu);
            total += s.bytes;
        }
        return total;
    }

private:
    struct InFlight {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        bool failed = false;
        ShardedLruCache::ValuePtr value;
        void set(ShardedLruCache::ValuePtr v) {
            { std::lock_guard<std::mutex> l(m); value = std::move(v); done = true; }
            cv.notify_all();
        }
        void fail() {
            { std::lock_guard<std::mutex> l(m); failed = true; done = true; }
            cv.notify_all();
        }
        ShardedLruCache::ValuePtr wait() {
            std::unique_lock<std::mutex> l(m);
            cv.wait(l, [&] { return done; });
            if (failed) throw std::runtime_error("single-flight load failed");
            return value;
        }
    };

    using Entry = std::pair<uint64_t, ValuePtr>;  // (key, value)
    struct Shard {
        mutable std::mutex mu;
        std::list<Entry> lru;  // front = MRU
        std::unordered_map<uint64_t, std::list<Entry>::iterator> map;
        std::unordered_map<uint64_t, std::shared_ptr<InFlight>> inflight;
        size_t bytes = 0;
    };

    void insert_locked(Shard& s, uint64_t key, const ValuePtr& val) {
        if (auto it = s.map.find(key); it != s.map.end()) {
            s.bytes -= it->second->second->size();
            s.bytes += val->size();
            it->second->second = val;
            s.lru.splice(s.lru.begin(), s.lru, it->second);
            return;
        }
        s.lru.emplace_front(key, val);
        s.map[key] = s.lru.begin();
        s.bytes += val->size();
        while (s.bytes > per_shard_cap_ && s.lru.size() > 1) {
            Entry& back = s.lru.back();
            s.bytes -= back.second->size();
            s.map.erase(back.first);
            s.lru.pop_back();
        }
    }

    size_t shards_;
    size_t per_shard_cap_;
    std::vector<Shard> tab_;
};

}  // namespace bs
