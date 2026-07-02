// bench.cpp — throughput / latency for the record store across access patterns.
//
// Matrix (kept deliberately small — not every combination):
//   sizes:   4 KiB and 1 MiB
//   writes:  sequential c=1, concurrent c=N
//   reads:   sequential (c=1, c=N) and random (c=1, c=N), each cold
//            4 KiB additionally splits random into uniform vs hot/cold
//            (different random sub-patterns are benchmarked only at 4 KiB).
//
// Each cold read phase reopens the reader and drops the file's pages from the OS
// page cache (evict_os_cache) so misses actually reach the device — otherwise reads
// would just measure RAM. The hot/cold phase intentionally lets its hot set warm up.
//
// Usage: bench [--dir PATH] [--threads N] [--mb M]
#include "record_reader.h"
#include "record_writer.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace bs;
using Clock = std::chrono::steady_clock;

static double now_s() {
    return std::chrono::duration<double>(Clock::now().time_since_epoch()).count();
}

// Latency samples in microseconds; percentiles computed on demand.
struct Lat {
    std::vector<double> us;
    void add(double v) { us.push_back(v); }
    void merge(const Lat& o) { us.insert(us.end(), o.us.begin(), o.us.end()); }
    double pct(double p) {
        if (us.empty()) return 0;
        std::sort(us.begin(), us.end());
        return us[(size_t)(p / 100.0 * (us.size() - 1))];
    }
};

static void row(const char* name, size_t block, int threads, uint64_t ops,
                double secs, Lat& lat) {
    double mbps = (double)ops * block / (1024.0 * 1024.0) / secs;
    std::printf("%-16s %6zuB  t=%-3d %10.1f MB/s  %11.0f op/s  p50=%7.1fus  p99=%9.1fus\n",
                name, block, threads, mbps, ops / secs, lat.pct(50), lat.pct(99));
}

// Run `total` iterations of fn(i) split across `threads`; each thread times its own
// ops (no shared latency vector, so timing doesn't add contention). Returns wall
// seconds and the merged latency distribution.
template <class Fn>
static std::pair<double, Lat> run(int threads, uint64_t total, Fn&& fn) {
    std::vector<Lat> locals(threads);
    std::vector<std::thread> ts;
    uint64_t per = total / threads;
    double t0 = now_s();
    for (int t = 0; t < threads; ++t)
        ts.emplace_back([&, t] {
            uint64_t b = (uint64_t)t * per, e = (t == threads - 1) ? total : b + per;
            Lat& L = locals[t];
            for (uint64_t i = b; i < e; ++i) {
                double a = now_s();
                fn(i);
                L.add((now_s() - a) * 1e6);
            }
        });
    for (auto& th : ts) th.join();
    double secs = now_s() - t0;
    Lat all;
    for (auto& L : locals) all.merge(L);
    return {secs, std::move(all)};
}

static void run_size(const std::string& dir, size_t block, uint64_t ops, int N) {
    std::string sdir = dir + "/sz" + std::to_string(block);
    (void)std::system(("rm -rf '" + sdir + "'").c_str());

    Options opt;
    opt.dir = sdir;
    opt.durability = Durability::OsBuffered;   // isolate raw I/O; close() still fsyncs
    opt.segment_size = 0xFFFFFFFFull;           // single segment, max capacity

    std::string payload(block, 'Z');
    std::vector<Index> handles(ops);

    // ---- WRITE: sequential, single thread (also populates the read store) ----
    {
        auto w = RecordWriter::open(opt);
        auto [secs, lat] = run(1, ops, [&](uint64_t i) { handles[i] = w->append(payload); });
        w->close();
        row("write-seq", block, 1, ops, secs, lat);
    }
    // ---- WRITE: concurrent, N threads (separate throwaway store) ----
    {
        std::string wdir = sdir + "_w";
        (void)std::system(("rm -rf '" + wdir + "'").c_str());
        Options wopt = opt;
        wopt.dir = wdir;
        auto w = RecordWriter::open(wopt);
        std::vector<Index> h2(ops);
        auto [secs, lat] = run(N, ops, [&](uint64_t i) { h2[i] = w->append(payload); });
        w->close();
        row("write-conc", block, N, ops, secs, lat);
        (void)std::system(("rm -rf '" + wdir + "'").c_str());
    }

    // ---- access orders (generated up front, not timed) ----
    std::mt19937_64 rng(0xB0BABEEF);
    std::vector<uint32_t> seq(ops), uni(ops), hot;
    for (uint64_t i = 0; i < ops; ++i) seq[i] = (uint32_t)i;
    for (uint64_t i = 0; i < ops; ++i) uni[i] = (uint32_t)(rng() % ops);
    // hot/cold: 90% of reads hit the hottest 10% of keys, 10% uniform.
    {
        uint64_t hotN = std::max<uint64_t>(1, ops / 10);
        hot.resize(ops);
        for (uint64_t i = 0; i < ops; ++i)
            hot[i] = (uint32_t)((rng() % 100 < 90) ? (rng() % hotN) : (rng() % ops));
    }

    // Fresh reader with a cold page cache for an honest device-read measurement.
    auto cold = [&] {
        auto r = RecordReader::open(opt);
        r->evict_os_cache();
        return r;
    };
    auto read = [&](const char* name, const std::vector<uint32_t>& order, int threads) {
        auto rd = cold();
        auto [secs, lat] = run(threads, order.size(), [&](uint64_t i) {
            if (rd->load(handles[order[i]]).size() != block) std::abort();
        });
        row(name, block, threads, order.size(), secs, lat);
    };

    read("read-seq", seq, 1);
    read("read-seq", seq, N);
    read("read-rand", uni, 1);
    read("read-rand", uni, N);
    if (block == 4096) {                 // random sub-patterns only at 4 KiB
        read("read-rand-hot", hot, 1);
        read("read-rand-hot", hot, N);
    }
    std::printf("\n");
    (void)std::system(("rm -rf '" + sdir + "'").c_str());
}

int main(int argc, char** argv) {
    std::string dir = "/tmp/recordstore_bench";
    int N = (int)std::thread::hardware_concurrency();
    if (N < 2) N = 4;
    uint64_t target_mb = 256;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&] { return std::string(i + 1 < argc ? argv[++i] : ""); };
        if (a == "--dir") dir = next();
        else if (a == "--threads") N = std::stoi(next());
        else if (a == "--mb") target_mb = std::stoull(next());
        else if (a == "--help") {
            std::printf("usage: bench [--dir PATH] [--threads N] [--mb M]\n");
            return 0;
        }
    }

    std::printf("recordstore bench  N=%d  target=%lluMiB/size  dir=%s\n\n",
                N, (unsigned long long)target_mb, dir.c_str());

    for (size_t block : {size_t(4096), size_t(1) << 20}) {
        uint64_t ops = target_mb * 1024 * 1024 / block;
        ops = std::max<uint64_t>(ops, block >= (1u << 20) ? 512 : 1024);  // enough for stats
        ops = std::min<uint64_t>(ops, (uint64_t)(3.5 * 1024 * 1024 * 1024) / block);  // < 4 GiB seg
        run_size(dir, block, ops, N);
    }
    return 0;
}
