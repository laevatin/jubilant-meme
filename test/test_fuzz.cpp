// Property-based fuzzing of the store's reliability invariants (deterministic
// seeds so failures reproduce):
//   1. Roundtrip: append(x) then load(handle) == x, for arbitrary sizes.
//   2. Concurrency: many threads appending/loading concurrently never observe torn
//      or wrong bytes; everything survives a reopen.
//   3. Crash/corruption: under random on-disk faults, load() is always
//      correct-or-throws (never returns wrong bytes); pure trailing garbage loses
//      nothing.
#include "record_reader.h"
#include "record_writer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace bs;

struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_fuzz_") + name;
        (void)std::system(("rm -rf '" + path + "'").c_str());
    }
    ~TmpDir() { (void)std::system(("rm -rf '" + path + "'").c_str()); }
};

static std::string seg_path(const std::string& dir) { return dir + "/000001.seg"; }
static uint64_t fsize(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0 ? (uint64_t)st.st_size : 0;
}

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

// ---- Property 1: roundtrip ----
TEST(Fuzz, RoundtripArbitrarySizes) {
    TmpDir d("roundtrip");
    std::mt19937_64 rng(0xC0FFEE);
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;  // speed; same write path
    auto w = RecordWriter::open(opt);
    auto r = RecordReader::open(opt);

    std::vector<std::pair<Index, std::string>> kept;
    for (int i = 0; i < 2500; ++i) {
        auto blob = rand_blob(rng, rand_size(rng));
        auto idx = w->append(blob);
        ASSERT_EQ(idx.length, (uint32_t)blob.size());
        ASSERT_EQ(r->load(idx), blob);  // immediate readback through a separate reader
        if (blob.size() < 4096) kept.emplace_back(idx, std::move(blob));
    }

    w->sync();
    w->close();
    auto r2 = RecordReader::open(opt);
    for (auto& [idx, blob] : kept) EXPECT_EQ(r2->load(idx), blob);  // durable across reopen
}

// ---- Property 2: concurrency safety ----
TEST(Fuzz, ConcurrentAppendsLoadsNeverTear) {
    TmpDir d("concurrent");
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto w = RecordWriter::open(opt);
    auto r = RecordReader::open(opt);

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
                auto idx = w->append(blob);
                if (r->load(idx) != blob) bad = true;  // own readback
                if (!mine.empty() && (rng() & 7) == 0) {
                    auto& [pi, pb] = mine[rng() % mine.size()];
                    if (r->load(pi) != pb) bad = true;
                }
                mine.emplace_back(idx, std::move(blob));
            }
            std::lock_guard<std::mutex> lk(mu);
            for (auto& x : mine) all.push_back(std::move(x));
        });
    for (auto& th : ts) th.join();
    EXPECT_FALSE(bad.load());
    EXPECT_EQ(w->stats().appends, (uint64_t)(kThreads * kPerThread));

    // Every offset reserved must be distinct and non-overlapping.
    std::sort(all.begin(), all.end(),
              [](auto& a, auto& b) { return a.first.offset < b.first.offset; });
    for (size_t i = 1; i < all.size(); ++i) {
        uint64_t prev_end = all[i - 1].first.offset + 12 + all[i - 1].first.length + 4;
        EXPECT_GE(all[i].first.offset, prev_end);  // no two records share a byte range
    }

    w->sync();
    w->close();
    auto r2 = RecordReader::open(opt);
    for (auto& [idx, blob] : all) EXPECT_EQ(r2->load(idx), blob);  // all durable
}

// ---- Property 3: crash/corruption invariants ----
//   APPEND_TAIL  trailing garbage (the common partial-write crash)  -> lose nothing
//   FLIP_BITS    random bit flips inside records                    -> correct-or-throw
//   TRUNCATE     chop the segment at a random offset                -> correct-or-throw
TEST(Fuzz, LoadIsAlwaysCorrectOrThrowsUnderFaults) {
    std::mt19937_64 rng(0xBADF00D);
    const int kIters = 80;

    for (int it = 0; it < kIters; ++it) {
        TmpDir d(("crash_" + std::to_string(it)).c_str());
        Options opt{.dir = d.path};
        opt.durability = Durability::OsBuffered;

        std::vector<std::pair<Index, std::string>> recs;
        {
            auto w = RecordWriter::open(opt);
            int n = 5 + (int)(rng() % 30);
            for (int i = 0; i < n; ++i) {
                auto blob = rand_blob(rng, 1 + rng() % 4096);
                recs.emplace_back(w->append(blob), std::move(blob));
            }
            w->sync();   // everything durable before the "crash"
            w->close();
        }

        std::string target = seg_path(d.path);
        enum { APPEND_TAIL, FLIP_BITS, TRUNCATE } fault = (decltype(fault))(rng() % 3);

        if (fault == APPEND_TAIL) {
            auto g = rand_blob(rng, 1 + rng() % 200);
            int fd = ::open(target.c_str(), O_WRONLY | O_APPEND);
            ASSERT_GE(fd, 0);
            ASSERT_EQ(::write(fd, g.data(), g.size()), (ssize_t)g.size());
            ::close(fd);
        } else if (fault == FLIP_BITS) {
            uint64_t sz = fsize(target);
            if (sz == 0) continue;
            int fd = ::open(target.c_str(), O_RDWR);
            ASSERT_GE(fd, 0);
            int flips = 1 + (int)(rng() % 5);
            for (int k = 0; k < flips; ++k) {
                uint64_t off = rng() % sz;
                char byte = 0;
                if (::pread(fd, &byte, 1, (off_t)off) == 1) {
                    byte ^= char(1u << (rng() % 8));
                    ASSERT_EQ(::pwrite(fd, &byte, 1, (off_t)off), 1);
                }
            }
            ::close(fd);
        } else {  // TRUNCATE
            uint64_t sz = fsize(target);
            ::truncate(target.c_str(), (off_t)(rng() % (sz + 1)));
        }

        auto w = RecordWriter::open(opt);   // recover (truncate torn tail)
        auto r = RecordReader::open(opt);
        for (auto& [idx, blob] : recs) {
            std::string got;
            bool threw = false;
            try { got = r->load(idx); } catch (...) { threw = true; }
            if (!threw) {
                // THE core invariant: if it returns, it must be exact.
                EXPECT_EQ(got, blob) << "load returned WRONG bytes under fault " << (int)fault;
            } else if (fault == APPEND_TAIL) {
                ADD_FAILURE() << "APPEND_TAIL lost a record that should survive";
            }
        }
        auto fresh = w->append("post-recovery");  // store remains usable
        EXPECT_EQ(r->load(fresh), std::string("post-recovery"));
    }
}
