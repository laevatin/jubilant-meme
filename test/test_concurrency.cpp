// Spec: thread-safety of the writer/reader contract. RecordWriter advertises
// concurrent append(); RecordReader advertises concurrent load()/scan() and is
// safe to use alongside a live writer. These tests gate those claims: no torn or
// wrong bytes, reserved byte ranges never overlap, and a scan run during writes
// only ever yields complete, CRC-valid records. Best run under TSan/ASan.
#include "record_reader.h"
#include "record_writer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace bs;

// Test-only failpoint defined in record_writer.cpp (see CloseWaitsForInFlightAppend...).
namespace bs { namespace detail { void set_after_reserve_hook(void (*)()); } }

struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_conc_") + name;
        (void)std::system(("rm -rf '" + path + "'").c_str());
    }
    ~TmpDir() { (void)std::system(("rm -rf '" + path + "'").c_str()); }
};

// A value whose content (length + bytes) is fully determined by (thread, seq), so
// a mismatch on load means torn/wrong bytes. Variable length stresses the framing.
static std::string make_value(int t, int i) {
    size_t n = 8 + (size_t)((t * 131 + i) % 4000);
    std::string s(n, char('a' + ((t + i) % 26)));
    uint32_t k = (uint32_t)(t * 100000 + i);              // identity in the first 4 bytes
    for (int b = 0; b < 4; ++b) s[b] = char((k >> (8 * b)) & 0xFF);
    return s;
}

// Concurrent writers: every reserved range is distinct and non-overlapping, and
// every record loads back to exactly what its writer wrote.
TEST(Concurrency, WritersReserveDisjointRanges) {
    TmpDir d("writers");
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto w = RecordWriter::open(opt);

    const int kThreads = 8, kPer = 600;
    std::mutex mu;
    std::vector<std::pair<Index, std::string>> all;

    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] {
            std::vector<std::pair<Index, std::string>> mine;
            for (int i = 0; i < kPer; ++i) {
                auto v = make_value(t, i);
                mine.emplace_back(w->append(v), std::move(v));
            }
            std::lock_guard<std::mutex> lk(mu);
            for (auto& x : mine) all.push_back(std::move(x));
        });
    for (auto& th : ts) th.join();

    ASSERT_EQ(all.size(), (size_t)(kThreads * kPer));
    EXPECT_EQ(w->stats().appends, (uint64_t)(kThreads * kPer));

    std::sort(all.begin(), all.end(),
              [](auto& a, auto& b) { return a.first.offset < b.first.offset; });
    for (size_t i = 1; i < all.size(); ++i) {
        uint64_t prev_end = all[i - 1].first.offset + 12 + all[i - 1].first.length + 4;
        EXPECT_GE(all[i].first.offset, prev_end);
    }

    auto r = RecordReader::open(opt);
    for (auto& [idx, v] : all) EXPECT_EQ(r->load(idx), v);
}

// Cross-thread read-your-writes: writers publish handles to a shared list while
// reader threads concurrently load random published handles and verify bytes.
TEST(Concurrency, ReadersSeeWritersRecordsConcurrently) {
    TmpDir d("rw");
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto w = RecordWriter::open(opt);
    auto r = RecordReader::open(opt);

    std::mutex mu;
    std::vector<std::pair<Index, std::string>> pub;
    std::atomic<bool> done{false};
    std::atomic<bool> bad{false};

    const int kWriters = 4, kPer = 800;
    std::vector<std::thread> ts;
    for (int t = 0; t < kWriters; ++t)
        ts.emplace_back([&, t] {
            for (int i = 0; i < kPer; ++i) {
                auto v = make_value(t, i);
                auto idx = w->append(v);
                std::lock_guard<std::mutex> lk(mu);
                pub.emplace_back(idx, std::move(v));
            }
        });

    const int kReaders = 4;
    std::vector<std::thread> rs;
    for (int t = 0; t < kReaders; ++t)
        rs.emplace_back([&, t] {
            std::mt19937_64 rng(7000 + t);
            while (!done.load()) {
                Index idx; std::string want;
                {
                    std::lock_guard<std::mutex> lk(mu);
                    if (pub.empty()) continue;
                    auto& e = pub[rng() % pub.size()];
                    idx = e.first; want = e.second;
                }
                if (r->load(idx) != want) bad = true;
            }
        });

    for (auto& th : ts) th.join();
    done.store(true);
    for (auto& th : rs) th.join();
    EXPECT_FALSE(bad.load());
}

// A scan run concurrently with a writer must only yield complete, CRC-valid records
// (CRC is checked inside scan), each matching what was written, never wrong bytes.
TEST(Concurrency, ScanDuringWritesYieldsConsistentPrefix) {
    TmpDir d("scan");
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto w = RecordWriter::open(opt);
    auto r = RecordReader::open(opt);

    std::mutex mu;
    std::unordered_map<uint64_t, std::string> expect;  // offset -> value
    std::atomic<bool> done{false};
    std::atomic<bool> bad{false};

    const int kTotal = 4000;
    std::thread writer([&] {
        for (int i = 0; i < kTotal; ++i) {
            auto v = make_value(0, i);
            auto idx = w->append(v);
            std::lock_guard<std::mutex> lk(mu);
            expect.emplace(idx.offset, std::move(v));
        }
        done.store(true);
    });

    size_t max_seen = 0;
    while (!done.load()) {
        size_t seen = 0;
        for (const auto& rec : r->scan()) {
            ++seen;
            std::lock_guard<std::mutex> lk(mu);
            auto it = expect.find(rec.index.offset);
            if (it != expect.end() && it->second != rec.value) bad = true;
        }
        max_seen = std::max(max_seen, seen);
    }
    writer.join();
    EXPECT_FALSE(bad.load());

    size_t final_seen = 0;
    for (const auto& rec : r->scan()) { (void)rec; ++final_seen; }
    EXPECT_EQ(final_seen, (size_t)kTotal);
    EXPECT_LE(max_seen, final_seen);
}

// Interleaved single appends and batched appends from many threads: all handles
// stay disjoint and load back.
TEST(Concurrency, InterleavedAppendAndAppendBatch) {
    TmpDir d("mixed");
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto w = RecordWriter::open(opt);

    std::mutex mu;
    std::vector<std::pair<Index, std::string>> all;
    std::vector<std::thread> ts;
    for (int t = 0; t < 8; ++t)
        ts.emplace_back([&, t] {
            std::mt19937_64 rng(9000 + t);
            std::vector<std::pair<Index, std::string>> mine;
            for (int i = 0; i < 300; ++i) {
                if (t % 2 == 0) {
                    auto v = make_value(t, i);
                    mine.emplace_back(w->append(v), std::move(v));
                } else {
                    int n = 1 + (int)(rng() % 8);
                    std::vector<std::string> vs;
                    for (int k = 0; k < n; ++k) vs.push_back(make_value(t, i * 100 + k));
                    auto idxs = w->appendBatch(vs);
                    for (int k = 0; k < n; ++k) mine.emplace_back(idxs[k], std::move(vs[k]));
                }
            }
            std::lock_guard<std::mutex> lk(mu);
            for (auto& x : mine) all.push_back(std::move(x));
        });
    for (auto& th : ts) th.join();

    std::sort(all.begin(), all.end(),
              [](auto& a, auto& b) { return a.first.offset < b.first.offset; });
    for (size_t i = 1; i < all.size(); ++i) {
        uint64_t prev_end = all[i - 1].first.offset + 12 + all[i - 1].first.length + 4;
        EXPECT_GE(all[i].first.offset, prev_end);
    }
    auto r = RecordReader::open(opt);
    for (auto& [idx, v] : all) EXPECT_EQ(r->load(idx), v);
}

// Deterministic close()/append() race via a test failpoint. The victim append parks
// between reserve() and pwrite() (inside the seam), still holding the shared lifetime
// lock, then close() runs. The correct code blocks close() on that shared lock until
// the append finishes, so the pwrite lands on a still-open fd. The old code (no
// lifetime lock) would ::close the fd while the append is parked, so when the victim
// resumes it pwrites a closed/recycled fd → EBADF. We pin that this never happens.
//
// (The seam is what makes this deterministic rather than a flaky timing race: the
// bad window — between reading seg_fd and issuing the pwrite — is a few instructions
// in production, far too narrow to hit reliably from black-box stress or even TSan.)
namespace {
std::atomic<bool> g_arm_park{false};
std::atomic<bool> g_victim_parked{false};
std::atomic<bool> g_release_victim{false};
void victim_pause_hook() {
    bool expected = true;
    if (!g_arm_park.compare_exchange_strong(expected, false)) return;  // only the first append parks
    g_victim_parked.store(true);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);  // safety net
    while (!g_release_victim.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
}
}  // namespace

TEST(Concurrency, CloseWaitsForInFlightAppendInsteadOfClosingFdUnderIt) {
    TmpDir d("closeseam");
    Options opt{.dir = d.path};
    opt.durability = Durability::OsBuffered;
    auto w = RecordWriter::open(opt);

    g_arm_park.store(true);
    g_victim_parked.store(false);
    g_release_victim.store(false);
    bs::detail::set_after_reserve_hook(&victim_pause_hook);

    std::atomic<bool> fd_misuse{false};
    std::string err;
    Index victim_idx{};
    std::thread victim([&] {
        try { victim_idx = w->append(std::string(4096, 'V')); }
        catch (const std::exception& e) { fd_misuse = true; err = e.what(); }
    });

    while (!g_victim_parked.load()) std::this_thread::yield();  // append is parked mid-flight

    std::thread closer([&] { w->close(); });  // runs while the append is parked

    // A buggy close() would ::close the fd within this window; a correct one blocks on
    // the shared lifetime lock and makes no such progress.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    g_release_victim.store(true);  // resume the victim -> it pwrites now
    victim.join();
    closer.join();
    bs::detail::set_after_reserve_hook(nullptr);

    EXPECT_FALSE(fd_misuse.load()) << "append used the fd after close(): " << err;

    // The append was admitted before close(), so it must be durable.
    if (!fd_misuse.load()) {
        auto r = RecordReader::open(opt);
        EXPECT_EQ(r->load(victim_idx), std::string(4096, 'V'));
    }
}

// Durability under concurrency: GroupCommit must make every acked concurrent
// append survive a clean close + reopen.
TEST(Concurrency, GroupCommitAckedAppendsSurviveReopen) {
    TmpDir d("durable");
    Options opt{.dir = d.path};
    opt.durability = Durability::GroupCommit;
    std::mutex mu;
    std::vector<std::pair<Index, std::string>> all;
    {
        auto w = RecordWriter::open(opt);
        std::vector<std::thread> ts;
        for (int t = 0; t < 6; ++t)
            ts.emplace_back([&, t] {
                std::vector<std::pair<Index, std::string>> mine;
                for (int i = 0; i < 300; ++i) {
                    auto v = make_value(t, i);
                    mine.emplace_back(w->append(v), std::move(v));  // returns only when durable
                }
                std::lock_guard<std::mutex> lk(mu);
                for (auto& x : mine) all.push_back(std::move(x));
            });
        for (auto& th : ts) th.join();
        w->close();
    }
    auto r = RecordReader::open(opt);
    for (auto& [idx, v] : all) EXPECT_EQ(r->load(idx), v);  // all durable
}
