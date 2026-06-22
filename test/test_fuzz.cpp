// Property-based fuzzing of the store's reliability invariants.
//
// Three properties, each over many randomized iterations (deterministic seeds so
// failures reproduce):
//   1. Roundtrip: store(x) then load(handle) == x, for arbitrary sizes, across
//      segment rotations.
//   2. Concurrency: many threads storing/loading concurrently never observe torn
//      or wrong bytes; everything survives a reopen.
//   3. Crash/corruption: under random on-disk faults, load() is always
//      correct-or-throws (never returns wrong bytes); and a pure trailing-garbage
//      fault loses nothing.
#include "blob_store.h"
#include "test_framework.h"

#include <atomic>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace bs;
namespace fs = std::filesystem;

struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_fuzz_") + name;
        (void)std::system(("rm -rf '" + path + "'").c_str());
    }
    ~TmpDir() { (void)std::system(("rm -rf '" + path + "'").c_str()); }
};

// Heavy-tailed size distribution: lots of tiny/0, some 4K-ish, occasional 1M.
static size_t rand_size(std::mt19937_64& rng) {
    uint32_t b = rng() % 100;
    if (b < 25) return rng() % 16;
    if (b < 55) return 1 + rng() % 256;
    if (b < 85) return 1 + rng() % 8192;
    if (b < 97) return 1 + rng() % 65536;
    return 1 + rng() % (1u << 20);
}
static std::string rand_blob(std::mt19937_64& rng, size_t n) {
    std::string s(n, '\0');
    for (auto& c : s) c = char(rng() & 0xFF);
    return s;
}

static std::vector<uint32_t> seg_ids(const std::string& dir) {
    std::vector<uint32_t> ids;
    for (auto& e : fs::directory_iterator(dir)) {
        auto n = e.path().filename().string();
        if (n.size() == 10 && n.substr(6) == ".seg")
            ids.push_back((uint32_t)std::stoul(n.substr(0, 6)));
    }
    return ids;
}
static uint64_t fsize(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0 ? (uint64_t)st.st_size : 0;
}

// ---- Property 1: roundtrip across rotations ----
TEST("fuzz: roundtrip for arbitrary sizes across rotations") {
    TmpDir d("roundtrip");
    std::mt19937_64 rng(0xC0FFEE);
    BlobStore::Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;  // speed; same code path
    auto store = BlobStore::open(opt);

    std::vector<std::pair<Index, std::string>> kept;
    for (int i = 0; i < 2500; ++i) {
        auto blob = rand_blob(rng, rand_size(rng));
        auto idx = store->store(blob);
        CHECK_EQ(idx.length, (uint32_t)blob.size());
        CHECK_EQ(*store->load(idx), blob);  // immediate readback
        if (blob.size() < 4096) kept.emplace_back(idx, std::move(blob));
    }

    // Durability across a reopen for the retained sample.
    store->sync();
    store.reset();
    auto store2 = BlobStore::open(opt);
    for (auto& [idx, blob] : kept) CHECK_EQ(*store2->load(idx), blob);
}

// ---- Property 2: concurrency safety ----
TEST("fuzz: concurrent stores/loads never tear; survive reopen") {
    TmpDir d("concurrent");
    BlobStore::Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto store = BlobStore::open(opt);

    const int kThreads = 8, kPerThread = 400;
    std::mutex mu;
    std::vector<std::pair<Index, std::string>> all;
    std::atomic<bool> bad{false};

    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] {
            std::mt19937_64 rng(1000 + t);
            std::vector<std::pair<Index, std::string>> mine;
            for (int i = 0; i < kPerThread; ++i) {
                auto blob = rand_blob(rng, rng() % 20000);
                auto idx = store->store(blob);
                if (*store->load(idx) != blob) bad = true;  // own readback
                // Occasionally re-read an earlier blob of ours.
                if (!mine.empty() && (rng() & 7) == 0) {
                    auto& [pi, pb] = mine[rng() % mine.size()];
                    if (*store->load(pi) != pb) bad = true;
                }
                mine.emplace_back(idx, std::move(blob));
            }
            std::lock_guard<std::mutex> lk(mu);
            for (auto& x : mine) all.push_back(std::move(x));
        });
    for (auto& th : ts) th.join();
    CHECK(!bad.load());
    CHECK_EQ(store->stats().stores, (uint64_t)(kThreads * kPerThread));

    store->sync();
    store.reset();
    auto store2 = BlobStore::open(opt);
    for (auto& [idx, blob] : all) CHECK_EQ(*store2->load(idx), blob);  // all durable
}

// ---- Property 3: crash/corruption invariants ----
//
// Faults applied to the closed store's files:
//   APPEND_TAIL  trailing garbage (the common partial-write crash)  -> lose nothing
//   FLIP_BITS    random bit flips inside records                    -> correct-or-throw
//   TRUNCATE     chop a segment at a random offset                  -> correct-or-throw
TEST("fuzz: load is always correct-or-throws under random faults") {
    std::mt19937_64 rng(0xBADF00D);
    const int kIters = 80;

    for (int it = 0; it < kIters; ++it) {
        TmpDir d(("crash_" + std::to_string(it)).c_str());
        BlobStore::Options opt{.dir = d.path};
        opt.durability = Durability::OsBuffered;

        std::vector<std::pair<Index, std::string>> recs;
        {
            auto store = BlobStore::open(opt);
            int n = 5 + (int)(rng() % 30);
            for (int i = 0; i < n; ++i) {
                auto blob = rand_blob(rng, 1 + rng() % 4096);
                recs.emplace_back(store->store(blob), std::move(blob));
            }
            store->sync();  // everything durable before the "crash"
        }

        auto ids = seg_ids(d.path);
        std::string target = d.path + "/" +
            [&] { char b[32]; std::snprintf(b, sizeof(b), "%06u.seg", ids[rng() % ids.size()]); return std::string(b); }();

        enum { APPEND_TAIL, FLIP_BITS, TRUNCATE } fault = (decltype(fault))(rng() % 3);

        if (fault == APPEND_TAIL) {
            auto g = rand_blob(rng, 1 + rng() % 200);
            int fd = ::open(target.c_str(), O_WRONLY | O_APPEND);
            CHECK(fd >= 0);
            CHECK(::write(fd, g.data(), g.size()) == (ssize_t)g.size());
            ::close(fd);
        } else if (fault == FLIP_BITS) {
            uint64_t sz = fsize(target);
            if (sz == 0) continue;
            int fd = ::open(target.c_str(), O_RDWR);
            CHECK(fd >= 0);
            int flips = 1 + (int)(rng() % 5);
            for (int k = 0; k < flips; ++k) {
                uint64_t off = rng() % sz;
                char byte = 0;
                if (::pread(fd, &byte, 1, (off_t)off) == 1) {
                    byte ^= char(1u << (rng() % 8));
                    CHECK(::pwrite(fd, &byte, 1, (off_t)off) == 1);
                }
            }
            ::close(fd);
        } else {  // TRUNCATE
            uint64_t sz = fsize(target);
            ::truncate(target.c_str(), (off_t)(rng() % (sz + 1)));
        }

        auto store = BlobStore::open(opt);
        for (auto& [idx, blob] : recs) {
            std::shared_ptr<const std::string> got;
            bool threw = false;
            try { got = store->load(idx); } catch (...) { threw = true; }
            if (!threw) {
                // THE core reliability invariant: if it returns, it must be exact.
                if (*got != blob)
                    throw tf::AssertFail("load returned WRONG bytes under fault " +
                                         std::to_string((int)fault));
            } else if (fault == APPEND_TAIL) {
                // Trailing garbage must never cost us a durably-stored record.
                throw tf::AssertFail("APPEND_TAIL lost a record that should survive");
            }
        }
        // Store remains usable after recovery.
        auto fresh = store->store(std::string_view("post-recovery"));
        CHECK_EQ(*store->load(fresh), std::string("post-recovery"));
    }
}

RUN_ALL_TESTS()
