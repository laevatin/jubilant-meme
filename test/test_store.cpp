// Spec: the BlobStore store/load contract — roundtrip, arbitrary length,
// handle serialization, opacity, durability, stats, and bad-handle rejection.
#include "blob_store.h"
#include "test_framework.h"

#include <cstdio>
#include <random>
#include <string>
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

TEST("segment rotation: many writes span multiple segments") {
    TmpDir d("rotate");
    BlobStore::Options opt{.dir = d.path};
    opt.segment_size = 64 * 1024;  // tiny segments to force rotation
    auto store = BlobStore::open(opt);
    std::mt19937_64 rng(7);
    std::vector<std::pair<Index, std::string>> kept;
    for (int i = 0; i < 200; ++i) {
        auto blob = rand_blob(rng, 1000);
        kept.push_back({store->store(blob), blob});
    }
    CHECK(store->stats().segments >= 2);
    for (auto& [idx, blob] : kept)
        CHECK_EQ(*store->load(idx), blob);
}

RUN_ALL_TESTS()
