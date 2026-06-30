// record_writer.cpp — append path + durability + crash recovery.
#include "record_writer.h"

#include "record_format.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace bs {

using namespace fmt;

// Test-only failpoint: invoked inside append_records between the offset reservation
// and the pwrite, while the shared lifetime lock is held. It lets a test park an
// append mid-flight to exercise the close()/append() interleaving deterministically.
// Cost in production is a single relaxed load + an (almost always not-taken) branch;
// the pointer is null unless a test installs one via bs::detail::set_after_reserve_hook.
namespace {
std::atomic<void (*)()> g_after_reserve_hook{nullptr};
}
namespace detail {
void set_after_reserve_hook(void (*hook)()) { g_after_reserve_hook.store(hook); }
}

struct RecordWriter::Impl {
    Options opts;                 // immutable after open()

    // ---- seg_fd lifetime: io_mu -------------------------------------------
    // io_mu guards the *lifetime* of the file descriptors, not their contents.
    // Appends/sync hold it SHARED for their whole body (reserve+pwrite+commit), so
    // close()'s EXCLUSIVE lock waits for every in-flight op to finish and blocks new
    // ones before it closes the fd — nothing can pwrite/fsync a closed/recycled fd.
    std::shared_mutex io_mu;
    int seg_fd = -1;              // guarded by io_mu (read=shared on use, write=exclusive in close)
    int dir_fd = -1;              // guarded by io_mu (closed under exclusive)

    // ---- append point: append_mu ------------------------------------------
    // A short critical section taken *inside* the shared io_mu, just to hand out
    // disjoint byte ranges and observe the closed flag.
    std::mutex append_mu;
    uint64_t cursor = 0;          // guarded by append_mu
    bool closed = false;          // guarded by append_mu

    // ---- group-commit queue: commit_mu / commit_cv ------------------------
    struct Waiter {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;        // guarded by Waiter::m
        void wait() { std::unique_lock<std::mutex> l(m); cv.wait(l, [&] { return done; }); }
        // Notify *under* the lock: the Waiter is stack-allocated in commit(), so if
        // we signaled after releasing m, the woken thread could return and destroy
        // this condition_variable while notify_one() is still reading it (a real
        // use-after-free that ThreadSanitizer flags). Holding m until after notify
        // keeps the waiter parked on the mutex until we're done touching cv.
        void signal() { std::lock_guard<std::mutex> l(m); done = true; cv.notify_one(); }
    };
    std::mutex commit_mu;
    std::condition_variable commit_cv;
    std::vector<Waiter*> pending;             // guarded by commit_mu (GroupCommit waiters)

    // ---- lock-free (atomics / set-once) -----------------------------------
    std::atomic<uint64_t> unsynced_bytes{0};  // atomic; AsyncFlush bytes since last fsync
    std::thread syncer;                        // set at open(), joined at close()
    std::atomic<bool> running{false};          // atomic; syncer run flag
    std::atomic<uint64_t> n_appends{0}, n_bytes{0}, n_fsyncs{0};  // atomic stats

    void fsync_dir() { if (dir_fd >= 0) ::fsync(dir_fd); }

    void open_existing_or_create() {
        namespace fs = std::filesystem;
        fs::create_directories(opts.dir);
        dir_fd = ::open(opts.dir.c_str(), O_RDONLY | O_DIRECTORY);

        std::string path = segment_path(opts.dir);
        bool existed = fs::exists(path);
        seg_fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (seg_fd < 0) throw std::system_error(errno, std::generic_category(), "open segment");
        if (existed) {
            cursor = recover_tail(seg_fd);
        } else {
            cursor = 0;
            fsync_dir();  // make the new file's directory entry durable
        }
    }

    // Scan from offset 0 for the end of the last *framing-sound* record (magic ok,
    // fits the file, header len == footer len). The first framing break is a torn
    // tail (crash mid-append) and everything from there is truncated. A record that
    // is framing-sound but CRC-bad is interior bit-rot: left in place (load rejects
    // it) and stepped over, so one corrupt record never discards later records.
    uint64_t recover_tail(int fd) {
        uint64_t size = file_size(fd);
        uint64_t pos = 0;
        for (;;) {
            FrameHeader h = read_header(fd, pos, size);
            if (!h.ok) break;                                     // framing break -> tail tear
            // Framing-only recovery: verify just the footer (cheap) — the payload CRC
            // is left for load(), so an interior CRC-bad record is preserved, not torn.
            char footer[kFooterSize];
            if (pread_all(fd, footer, kFooterSize, pos + kHeaderSize + h.len) != kFooterSize) break;
            if (get_u32(footer) != h.len) break;                  // len copies disagree -> tear
            pos += h.reclen;
        }
        if (pos < size) ::ftruncate(fd, (off_t)pos);  // drop only the unframed tail
        return pos;
    }

    struct Reservation { uint64_t off; };

    // Reserve a contiguous region for `total` bytes at the append point.
    Reservation reserve(uint64_t total) {
        std::lock_guard<std::mutex> lk(append_mu);
        if (closed) throw std::runtime_error("writer is closed");
        if (cursor + total > opts.segment_size)
            throw std::runtime_error("store is full (segment_size exceeded)");
        Reservation r{cursor};
        cursor += total;
        return r;
    }

    // Make a just-written region (of `bytes` payload+framing) durable per the mode.
    void commit(uint64_t bytes) {
        switch (opts.durability) {
            case Durability::GroupCommit: {
                Waiter w;
                { std::lock_guard<std::mutex> lk(commit_mu); pending.push_back(&w); }
                commit_cv.notify_one();
                w.wait();  // blocks until the syncer fsyncs this batch
                break;
            }
            case Durability::Sync:
                ::fsync(seg_fd);
                n_fsyncs.fetch_add(1);
                break;
            case Durability::AsyncFlush: {
                uint64_t u = unsynced_bytes.fetch_add(bytes) + bytes;
                if (u >= opts.async_flush_bytes) commit_cv.notify_one();
                break;
            }
            case Durability::OsBuffered:
                break;
        }
    }

    // One syncer serves both background modes. GroupCommit drives it via queued
    // waiters; AsyncFlush via the size-or-interval threshold (signals no one).
    void syncer_loop() {
        const bool async = opts.durability == Durability::AsyncFlush;
        const uint64_t interval = async ? opts.async_flush_interval_us
                                        : opts.group_commit_interval_us;
        const uint64_t threshold = opts.async_flush_bytes;
        while (running.load()) {
            std::vector<Waiter*> batch;
            uint64_t bytes = 0;
            {
                std::unique_lock<std::mutex> lk(commit_mu);
                commit_cv.wait_for(lk, std::chrono::microseconds(interval), [&] {
                    return !pending.empty() || unsynced_bytes.load() >= threshold || !running.load();
                });
                batch.swap(pending);
                bytes = unsynced_bytes.exchange(0);
            }
            if (batch.empty() && bytes == 0) continue;
            flush_batch(batch);
        }
        // Drain on shutdown.
        std::vector<Waiter*> batch;
        uint64_t bytes = 0;
        { std::lock_guard<std::mutex> lk(commit_mu); batch.swap(pending); bytes = unsynced_bytes.exchange(0); }
        if (!batch.empty() || bytes > 0) flush_batch(batch);
    }

    void flush_batch(std::vector<Waiter*>& batch) {
        ::fsync(seg_fd);
        n_fsyncs.fetch_add(1);
        for (auto* w : batch) w->signal();
    }

    void start_syncer() {
        if (opts.durability == Durability::GroupCommit ||
            opts.durability == Durability::AsyncFlush) {
            running.store(true);
            syncer = std::thread([this] { syncer_loop(); });
        }
    }

    void stop_syncer() {
        if (running.exchange(false)) {
            commit_cv.notify_all();
            if (syncer.joinable()) syncer.join();
        }
    }

    // Frame, reserve, write, and commit one or more records. Payloads are written
    // straight from the caller's buffers via pwritev — only the small per-record
    // framing bytes (header+footer) are staged, never the payload (no memcpy of the
    // blob, which matters for large records and big batches).
    std::vector<Index> append_records(const std::vector<std::string>& values) {
        uint64_t total = 0;
        for (const auto& v : values) {
            if (v.size() > 0xFFFFFFFFull) throw std::invalid_argument("blob too large (>4GiB)");
            total += kHeaderSize + v.size() + kFooterSize;
        }
        // Stage only the framing bytes; reference each payload in place via iovec.
        std::string frames(values.size() * (kHeaderSize + kFooterSize), '\0');
        std::vector<struct iovec> iov;
        iov.reserve(values.size() * 3);
        for (size_t i = 0; i < values.size(); ++i) {
            const std::string& v = values[i];
            char* hdr = frames.data() + i * (kHeaderSize + kFooterSize);
            char* footer = hdr + kHeaderSize;
            frame_header(hdr, footer, v.data(), v.size());
            iov.push_back({hdr, (size_t)kHeaderSize});
            if (!v.empty()) iov.push_back({const_cast<char*>(v.data()), v.size()});  // read-only
            iov.push_back({footer, (size_t)kFooterSize});
        }

        // Hold io_mu shared across reserve+pwritev+commit so close() cannot pull the
        // fd out from under us. reserve() still throws if the store was closed first.
        std::shared_lock<std::shared_mutex> io_lock(io_mu);
        auto r = reserve(total);
        if (auto h = g_after_reserve_hook.load(std::memory_order_relaxed)) h();  // test seam
        pwritev_all(seg_fd, iov.data(), iov.size(), r.off);
        commit(total);

        std::vector<Index> out;
        out.reserve(values.size());
        uint64_t off = r.off, bytes = 0;
        for (const auto& v : values) {
            out.push_back(Index{kSegmentId, (uint32_t)v.size(), off});
            off += kHeaderSize + v.size() + kFooterSize;
            bytes += v.size();
        }
        n_appends.fetch_add(values.size());
        n_bytes.fetch_add(bytes);
        return out;
    }
};

// ---- public methods --------------------------------------------------------

RecordWriter::RecordWriter() : p_(std::make_unique<Impl>()) {}
RecordWriter::~RecordWriter() { close(); }

std::unique_ptr<RecordWriter> RecordWriter::open(const Options& opts) {
    if (opts.segment_size == 0 || opts.segment_size > kMaxSegmentSize)
        throw std::invalid_argument("segment_size must be in (0, 4GiB)");
    std::unique_ptr<RecordWriter> w(new RecordWriter());
    w->p_->opts = opts;
    w->p_->open_existing_or_create();
    w->p_->start_syncer();
    return w;
}

Index RecordWriter::append(const std::string& value) {
    return p_->append_records({value}).front();
}

std::vector<Index> RecordWriter::appendBatch(const std::vector<std::string>& values) {
    if (values.empty()) return {};
    return p_->append_records(values);
}

void RecordWriter::sync() {
    Impl& s = *p_;
    std::shared_lock<std::shared_mutex> io_lock(s.io_mu);  // keep seg_fd alive vs close()
    if (s.seg_fd >= 0) { ::fsync(s.seg_fd); s.n_fsyncs.fetch_add(1); }
}

void RecordWriter::close() {
    if (!p_) return;
    Impl& s = *p_;
    // Exclusive: blocks until every in-flight append has released its shared lock,
    // and prevents new ones from starting, so we never close a fd someone is using.
    // Exclusive: blocks until every in-flight append has released its shared lock,
    // and prevents new ones from starting, so we never close a fd someone is using.
    std::unique_lock<std::shared_mutex> io_lock(s.io_mu);
    { std::lock_guard<std::mutex> lk(s.append_mu); if (s.closed) return; s.closed = true; }
    s.stop_syncer();
    if (s.seg_fd >= 0) { ::fsync(s.seg_fd); s.n_fsyncs.fetch_add(1); ::close(s.seg_fd); s.seg_fd = -1; }
    if (s.dir_fd >= 0) { ::close(s.dir_fd); s.dir_fd = -1; }
}

RecordWriter::Stats RecordWriter::stats() const {
    Impl& s = *p_;
    Stats st;
    st.appends = s.n_appends.load();
    st.bytes_written = s.n_bytes.load();
    st.fsyncs = s.n_fsyncs.load();
    st.segments = 1;
    return st;
}

}  // namespace bs
