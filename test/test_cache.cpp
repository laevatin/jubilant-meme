// Spec: sharded LRU caches, evicts under capacity pressure, and coalesces
// concurrent misses on the same key onto a single loader call (single-flight).
#include "lru_cache.h"
#include "test_framework.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using bs::ShardedLruCache;
using VP = ShardedLruCache::ValuePtr;

static VP make(const std::string& s) { return std::make_shared<const std::string>(s); }

TEST("cache hit returns stored value and reports hit") {
    ShardedLruCache c(1 << 20, 4);
    int loads = 0;
    auto loader = [&] { ++loads; return make("hello"); };

    ShardedLruCache::Outcome o1, o2;
    auto a = c.get_or_load(42, loader, &o1);
    auto b = c.get_or_load(42, loader, &o2);

    CHECK_EQ(*a, std::string("hello"));
    CHECK_EQ(*b, std::string("hello"));
    CHECK_EQ(loads, 1);        // second call served from cache
    CHECK(!o1.hit);
    CHECK(o2.hit);
}

TEST("cache evicts LRU entries past capacity") {
    // 1 shard, capacity 100 bytes; 10-byte values => ~10 resident max.
    ShardedLruCache c(100, 1);
    for (int i = 0; i < 50; ++i)
        c.put(i, make(std::string(10, 'a' + (i % 26))));
    CHECK(c.size_bytes() <= 100);
    CHECK(!c.contains(0));   // earliest evicted
    CHECK(c.contains(49));   // newest retained
}

TEST("single-flight collapses concurrent misses to one load") {
    ShardedLruCache c(1 << 20, 8);
    std::atomic<int> active_loads{0};
    std::atomic<int> max_concurrent{0};
    std::atomic<int> total_loads{0};

    auto loader = [&]() -> VP {
        int now = ++active_loads;
        int prev = max_concurrent.load();
        while (now > prev && !max_concurrent.compare_exchange_weak(prev, now)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(50));  // hold the slot
        ++total_loads;
        --active_loads;
        return make("v");
    };

    const int N = 32;
    std::atomic<int> coalesced{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < N; ++i)
        ts.emplace_back([&] {
            ShardedLruCache::Outcome o;
            auto v = c.get_or_load(7, loader, &o);
            CHECK_EQ(*v, std::string("v"));
            if (o.coalesced) ++coalesced;
        });
    for (auto& t : ts) t.join();

    CHECK_EQ(total_loads.load(), 1);     // loader ran exactly once
    CHECK_EQ(max_concurrent.load(), 1);  // never two loaders for one key
    CHECK(coalesced.load() >= 1);        // others waited on it
}

RUN_ALL_TESTS()
