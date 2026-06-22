// bench.cpp — random/sequential x read/write throughput & latency for 4K and 1M
// blocks, plus a cache-effect read benchmark (duplicate loads).
//
// Usage:
//   bench [--dir PATH] [--threads N] [--seconds-cap S]
//         [--durability group|sync|osbuffered] [--no-cache]
// With no args it runs the full matrix at 4K and 1M.
#include "blob_store.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace bs;
using Clock = std::chrono::steady_clock;
using std::chrono::duration;
using std::chrono::nanoseconds;

static double now_s() {
    return duration<double>(Clock::now().time_since_epoch()).count();
}

struct LatencyVec {
    std::vector<double> us;  // microseconds
    void add(double v) { us.push_back(v); }
    double pct(double p) {
        if (us.empty()) return 0;
        std::sort(us.begin(), us.end());
        size_t i = (size_t)(p / 100.0 * (us.size() - 1));
        return us[i];
    }
};

static void row(const char* name, size_t block, int threads, uint64_t ops,
                double secs, double bytes, LatencyVec& lat) {
    double mbps = bytes / (1024.0 * 1024.0) / secs;
    double iops = ops / secs;
    std::printf("%-22s %6zuB t=%-2d  %9.1f MB/s  %10.0f op/s  p50=%7.1fus p99=%8.1fus\n",
                name, block, threads, mbps, iops, lat.pct(50), lat.pct(99));
}

// Run `total` ops split across `threads`, calling fn(thread_id, op_index).
template <class Fn>
static double run_parallel(int threads, uint64_t total, Fn&& fn) {
    std::vector<std::thread> ts;
    uint64_t per = total / threads;
    double t0 = now_s();
    for (int t = 0; t < threads; ++t)
        ts.emplace_back([&, t] {
            uint64_t begin = (uint64_t)t * per;
            uint64_t end = (t == threads - 1) ? total : begin + per;
            for (uint64_t i = begin; i < end; ++i) fn(t, i);
        });
    for (auto& th : ts) th.join();
    return now_s() - t0;
}

struct Config {
    std::string dir = "/tmp/blobstore_bench";
    int threads = 4;
    Durability dur = Durability::GroupCommit;
    bool cache = true;
    double target_bytes_per_size = 512.0 * 1024 * 1024;  // ~512 MiB of data per block size
};

static void bench_one_size(const Config& cfg, size_t block) {
    // Keep op counts reasonable: cap totals so 4K doesn't explode.
    uint64_t ops = (uint64_t)(cfg.target_bytes_per_size / block);
    ops = std::max<uint64_t>(ops, 64);
    // Single segment caps a store at <4 GiB; keep seq+conc+batch writes under it.
    if (block >= (1u << 20)) ops = std::min<uint64_t>(ops, 1200);
    else ops = std::min<uint64_t>(ops, 200000);

    std::string dir = cfg.dir + "/sz" + std::to_string(block);
    (void)std::system(("rm -rf '" + dir + "'").c_str());

    BlobStore::Options opt;
    opt.dir = dir;
    opt.durability = cfg.dur;
    opt.enable_cache = cfg.cache;
    opt.segment_size = 0xFFFFFFFFull;  // single segment: max capacity (just under 4 GiB)
    auto store = BlobStore::open(opt);

    // Payload pool (one buffer reused; content irrelevant to timing).
    std::string payload(block, 'Z');

    // ---- WRITE: sequential (1 thread) ----
    std::vector<Index> handles(ops);
    {
        LatencyVec lat;
        std::mutex lm;
        double secs = run_parallel(1, ops, [&](int, uint64_t i) {
            double a = now_s();
            handles[i] = store->store(payload);
            double b = now_s();
            std::lock_guard<std::mutex> g(lm); lat.add((b - a) * 1e6);
        });
        row("seq-write", block, 1, ops, secs, double(ops) * block, lat);
    }

    // ---- WRITE: concurrent (N threads) — the "random/parallel" write analog ----
    {
        std::vector<Index> h2(ops);
        LatencyVec lat; std::mutex lm;
        double secs = run_parallel(cfg.threads, ops, [&](int, uint64_t i) {
            double a = now_s();
            h2[i] = store->store(payload);
            double b = now_s();
            std::lock_guard<std::mutex> g(lm); lat.add((b - a) * 1e6);
        });
        row("conc-write", block, cfg.threads, ops, secs, double(ops) * block, lat);
    }

    // ---- BATCH: appendBatch / loadBatch (amortized lock + one fsync per batch) ----
    {
        std::string bdir = dir + "_batch";
        (void)std::system(("rm -rf '" + bdir + "'").c_str());
        BlobStore::Options bopt = opt;
        bopt.dir = bdir;
        auto bstore = BlobStore::open(bopt);

        const uint64_t kBatch = 64;
        std::vector<Index> bhandles; bhandles.reserve(ops);

        LatencyVec wlat;
        double t0 = now_s();
        for (uint64_t i = 0; i < ops; i += kBatch) {
            uint64_t n = std::min<uint64_t>(kBatch, ops - i);
            std::vector<std::string_view> chunk(n, payload);
            double a = now_s();
            auto idxs = bstore->appendBatch(chunk);
            wlat.add((now_s() - a) * 1e6 / n);  // per-record latency
            for (auto& x : idxs) bhandles.push_back(x);
        }
        row("batch-write", block, 1, ops, now_s() - t0, double(ops) * block, wlat);

        bstore = BlobStore::open(bopt);  // fresh: cold batched reads
        LatencyVec rlat;
        double r0 = now_s();
        for (uint64_t i = 0; i < ops; i += kBatch) {
            uint64_t n = std::min<uint64_t>(kBatch, ops - i);
            std::vector<Index> ix(bhandles.begin() + i, bhandles.begin() + i + n);
            double a = now_s();
            auto vals = bstore->loadBatch(ix);
            rlat.add((now_s() - a) * 1e6 / n);
            if (vals.back()->size() != block) std::abort();
        }
        row("batch-read", block, 1, ops, now_s() - r0, double(ops) * block, rlat);
        (void)std::system(("rm -rf '" + bdir + "'").c_str());
    }

    // Fresh store (drop cache) for honest cold reads.
    store = BlobStore::open(opt);

    // ---- READ: sequential (handles in write order) ----
    {
        LatencyVec lat; std::mutex lm;
        double secs = run_parallel(cfg.threads, ops, [&](int, uint64_t i) {
            double a = now_s();
            auto v = store->load(handles[i]);
            double b = now_s();
            if (v->size() != block) std::abort();
            std::lock_guard<std::mutex> g(lm); lat.add((b - a) * 1e6);
        });
        row("seq-read", block, cfg.threads, ops, secs, double(ops) * block, lat);
    }

    // ---- READ: random (shuffled handles) ----
    std::vector<uint64_t> order(ops);
    for (uint64_t i = 0; i < ops; ++i) order[i] = i;
    std::mt19937_64 rng(99);
    std::shuffle(order.begin(), order.end(), rng);
    {
        LatencyVec lat; std::mutex lm;
        double secs = run_parallel(cfg.threads, ops, [&](int, uint64_t i) {
            double a = now_s();
            auto v = store->load(handles[order[i]]);
            double b = now_s();
            if (v->size() != block) std::abort();
            std::lock_guard<std::mutex> g(lm); lat.add((b - a) * 1e6);
        });
        row("rand-read", block, cfg.threads, ops, secs, double(ops) * block, lat);
    }

    // ---- READ: cached/duplicate (hammer a small hot set => cache + single-flight) ----
    {
        uint64_t hot = std::min<uint64_t>(ops, 64);
        uint64_t reads = ops;  // same op budget, but only `hot` distinct keys
        LatencyVec lat; std::mutex lm;
        double secs = run_parallel(cfg.threads, reads, [&](int, uint64_t i) {
            double a = now_s();
            auto v = store->load(handles[i % hot]);
            double b = now_s();
            if (v->size() != block) std::abort();
            std::lock_guard<std::mutex> g(lm); lat.add((b - a) * 1e6);
        });
        auto st = store->stats();
        row("rand-read-cached", block, cfg.threads, reads, secs, double(reads) * block, lat);
        std::printf("    cache: hits=%llu misses=%llu coalesced=%llu\n",
                    (unsigned long long)st.cache_hits, (unsigned long long)st.cache_misses,
                    (unsigned long long)st.coalesced);
    }
    std::printf("\n");
}

int main(int argc, char** argv) {
    Config cfg;
    std::vector<size_t> sizes = {4096, 1u << 20};
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&] { return std::string(i + 1 < argc ? argv[++i] : ""); };
        if (a == "--dir") cfg.dir = next();
        else if (a == "--threads") cfg.threads = std::stoi(next());
        else if (a == "--no-cache") cfg.cache = false;
        else if (a == "--durability") {
            std::string d = next();
            cfg.dur = d == "sync" ? Durability::Sync
                    : d == "osbuffered" ? Durability::OsBuffered
                    : Durability::GroupCommit;
        } else if (a == "--help") {
            std::printf("Usage: bench [--dir P] [--threads N] [--durability group|sync|osbuffered] [--no-cache]\n");
            return 0;
        }
    }
    const char* dn = cfg.dur == Durability::Sync ? "sync"
                   : cfg.dur == Durability::OsBuffered ? "osbuffered" : "group-commit";
    std::printf("blobstore bench  threads=%d  durability=%s  cache=%s\n\n",
                cfg.threads, dn, cfg.cache ? "on" : "off");
    for (size_t s : sizes) bench_one_size(cfg, s);
    return 0;
}
