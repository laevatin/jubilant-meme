// Spec: the BlobStore store/load contract — roundtrip, arbitrary length,
// handle serialization, opacity, durability, stats, and bad-handle rejection.
#include "blob_store.h"
#include "test_framework.h"

#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace bs;

// A unique scratch dir per test; removed on construction so each run is clean.
struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_test_") + name;
        std::string cmd = "rm -rf '" + path + "'";
        (void)std::system(cmd.c_str());
    }
    ~TmpDir() {
        std::string cmd = "rm -rf '" + path + "'";
        (void)std::system(cmd.c_str());
    }
};

static std::string rand_blob(std::mt19937_64& rng, size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) s[i] = char(rng() & 0xFF);
    return s;
}

TEST("store then load roundtrips bytes") {
    TmpDir d("roundtrip");
    auto store = BlobStore::open({.dir = d.path});
    auto idx = store->store(std::string_view("hello world"));
    CHECK(idx.valid());
    auto got = store->load(idx);
    CHECK_EQ(*got, std::string("hello world"));
}

TEST("distinct writes get distinct handles and values") {
    TmpDir d("distinct");
    auto store = BlobStore::open({.dir = d.path});
    auto a = store->store(std::string_view("aaa"));
    auto b = store->store(std::string_view("bbbb"));
    CHECK(a != b);
    CHECK_EQ(*store->load(a), std::string("aaa"));
    CHECK_EQ(*store->load(b), std::string("bbbb"));
}

TEST("arbitrary lengths: empty, tiny, 4K, 1M, odd sizes") {
    TmpDir d("arbitrary");
    auto store = BlobStore::open({.dir = d.path});
    std::mt19937_64 rng(123);
    std::vector<size_t> sizes = {0, 1, 7, 4096, 4097, 1u << 20, (1u << 20) + 333};
    std::vector<std::pair<Index, std::string>> kept;
    for (size_t n : sizes) {
        auto blob = rand_blob(rng, n);
        kept.push_back({store->store(blob), blob});
    }
    for (auto& [idx, blob] : kept) {
        CHECK_EQ(idx.length, (uint32_t)blob.size());
        CHECK_EQ(*store->load(idx), blob);
    }
}

TEST("index serializes to 16 bytes and back") {
    Index a{.segment = 3, .length = 4096, .offset = 123456789};
    auto bytes = a.bytes();
    CHECK_EQ(bytes.size(), (size_t)16);
    auto b = Index::from_bytes(bytes);
    CHECK_EQ(a, b);
}

TEST("default index is invalid") {
    Index z;
    CHECK(!z.valid());
}

TEST("handles survive store reopen (opaque handle is self-describing)") {
    TmpDir d("reopen");
    std::array<uint8_t, 16> saved{};
    {
        auto store = BlobStore::open({.dir = d.path});
        auto idx = store->store(std::string_view("persisted"));
        saved = idx.bytes();
        store->sync();
    }
    {
        auto store = BlobStore::open({.dir = d.path});
        auto idx = Index::from_bytes(saved);
        CHECK_EQ(*store->load(idx), std::string("persisted"));
    }
}

TEST("load rejects a corrupt/out-of-range handle") {
    TmpDir d("badhandle");
    auto store = BlobStore::open({.dir = d.path});
    store->store(std::string_view("real"));
    Index bogus{.segment = 1, .length = 10, .offset = 99999999};  // past EOF
    CHECK_THROWS(store->load(bogus));
}

TEST("stats count stores, loads, and cache hits") {
    TmpDir d("stats");
    auto store = BlobStore::open({.dir = d.path});
    auto idx = store->store(std::string_view("x"));
    store->load(idx);
    store->load(idx);  // second load should hit cache
    auto s = store->stats();
    CHECK_EQ(s.stores, (uint64_t)1);
    CHECK_EQ(s.loads, (uint64_t)2);
    CHECK(s.cache_hits >= 1);
}

TEST("store reports a single segment") {
    TmpDir d("onesegment");
    auto store = BlobStore::open({.dir = d.path});
    for (int i = 0; i < 50; ++i) store->store(std::string_view("data"));
    CHECK_EQ(store->stats().segments, (uint32_t)1);
}

TEST("store throws when the segment cap is exceeded") {
    TmpDir d("full");
    BlobStore::Options opt{.dir = d.path};
    opt.segment_size = 4096;  // tiny cap
    auto store = BlobStore::open(opt);
    store->store(std::string(2000, 'x'));  // fits
    CHECK_THROWS(store->store(std::string(4000, 'y')));  // would overflow the cap
}

TEST("appendBatch returns one handle per blob, all loadable") {
    TmpDir d("batch");
    auto store = BlobStore::open({.dir = d.path});
    std::vector<std::string> owned = {"", "a", std::string(4096, 'Z'), "tail",
                                      std::string(1234, '\x7F')};
    std::vector<std::string_view> views(owned.begin(), owned.end());

    auto idxs = store->appendBatch(views);
    CHECK_EQ(idxs.size(), owned.size());
    for (size_t i = 0; i < owned.size(); ++i) {
        CHECK_EQ(idxs[i].length, (uint32_t)owned[i].size());
        CHECK_EQ(*store->load(idxs[i]), owned[i]);
    }
    // Batch is counted as N stores.
    CHECK_EQ(store->stats().stores, (uint64_t)owned.size());
    // Empty batch is a no-op.
    CHECK(store->appendBatch({}).empty());
}

TEST("loadBatch returns values in order, survives reopen") {
    TmpDir d("loadbatch");
    std::vector<std::array<uint8_t, 16>> saved;
    std::vector<std::string> owned = {"one", "two", "three", std::string(5000, 'Q')};
    {
        auto store = BlobStore::open({.dir = d.path});
        std::vector<std::string_view> views(owned.begin(), owned.end());
        for (auto& idx : store->appendBatch(views)) saved.push_back(idx.bytes());
        store->sync();
    }
    auto store = BlobStore::open({.dir = d.path});
    std::vector<Index> idxs;
    for (auto& b : saved) idxs.push_back(Index::from_bytes(b));
    auto vals = store->loadBatch(idxs);
    CHECK_EQ(vals.size(), owned.size());
    for (size_t i = 0; i < owned.size(); ++i) CHECK_EQ(*vals[i], owned[i]);
}

TEST("AsyncFlush: background worker fsyncs on the size threshold, data survives reopen") {
    TmpDir d("async");
    std::array<uint8_t, 16> saved{};
    {
        BlobStore::Options opt{.dir = d.path};
        opt.durability = Durability::AsyncFlush;
        opt.async_flush_bytes = 64 * 1024;       // small size trigger
        opt.async_flush_interval_us = 100000;    // long interval => size trigger fires first
        auto store = BlobStore::open(opt);

        // Write well past the size threshold; stores return without blocking on fsync.
        for (int i = 0; i < 200; ++i) saved = store->store(std::string(1024, 'a')).bytes();

        // The background worker should fsync without any explicit sync() from us.
        bool flushed = false;
        for (int i = 0; i < 100 && !flushed; ++i) {
            if (store->stats().fsyncs > 0) flushed = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(flushed);
    }
    // Reopen (the prior store also flushed on close) and confirm durability.
    auto store = BlobStore::open({.dir = d.path});
    CHECK_EQ(*store->load(Index::from_bytes(saved)), std::string(1024, 'a'));
}

RUN_ALL_TESTS()
