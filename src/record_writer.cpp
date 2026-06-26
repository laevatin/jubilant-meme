// record_writer.cpp — append path + durability + crash recovery.
#include "record_writer.h"

#include "record_format.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace bs {

using namespace fmt;

struct RecordWriter::Impl {
    Options opts;
    int dir_fd = -1;

    // The single segment file and its append point.
    std::mutex append_mu;
    int seg_fd = -1;
    uint64_t cursor = 0;
    bool closed = false;

    // Group-commit syncer.
    struct Waiter {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        void wait() { std::unique_lock<std::mutex> l(m); cv.wait(l, [&] { return done; }); }
        void signal() { { std::lock_guard<std::mutex> l(m); done = true; } cv.notify_one(); }
    };
    std::mutex commit_mu;
    std::condition_variable commit_cv;
    std::vector<Waiter*> pending;             // GroupCommit: writers awaiting an fsync
    std::atomic<uint64_t> unsynced_bytes{0};  // AsyncFlush: bytes written since the last fsync
    std::thread syncer;
    std::atomic<bool> running{false};

    // Stats.
    std::atomic<uint64_t> n_appends{0}, n_bytes{0}, n_fsyncs{0};

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
        struct stat st{};
        if (::fstat(fd, &st) != 0) return 0;
        uint64_t size = (uint64_t)st.st_size;
        uint64_t pos = 0;
        std::string hdr(kHeaderSize, '\0');
        while (pos + kHeaderSize <= size) {
            if (pread_all(fd, hdr.data(), kHeaderSize, pos) != kHeaderSize) break;
            if (get_u32(hdr.data()) != kMagic) break;            // framing gone -> tail tear
            uint32_t len = get_u32(hdr.data() + 4);
            uint64_t reclen = kHeaderSize + len + kFooterSize;
            if (pos + reclen > size) break;                       // runs past EOF -> tail tear
            std::string footer(kFooterSize, '\0');
            if (pread_all(fd, footer.data(), kFooterSize, pos + kHeaderSize + len) != kFooterSize)
                break;
            if (get_u32(footer.data()) != len) break;             // len copies disagree -> tear
            pos += reclen;  // framing sound (CRC, if bad, is caught at load())
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

    // Frame, reserve, write, and commit one or more contiguous records.
    std::vector<Index> append_records(const std::vector<std::string>& values) {
        uint64_t total = 0;
        for (const auto& v : values) {
            if (v.size() > 0xFFFFFFFFull) throw std::invalid_argument("blob too large (>4GiB)");
            total += kHeaderSize + v.size() + kFooterSize;
        }
        std::string buf(total, '\0');
        uint64_t p = 0;
        for (const auto& v : values) p += frame_record(buf.data() + p, v.data(), v.size());

        auto r = reserve(total);
        pwrite_all(seg_fd, buf.data(), total, r.off);
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
    if (s.seg_fd >= 0) { ::fsync(s.seg_fd); s.n_fsyncs.fetch_add(1); }
}

void RecordWriter::close() {
    if (!p_) return;
    Impl& s = *p_;
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
