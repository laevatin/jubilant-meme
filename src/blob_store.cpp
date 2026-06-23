// blob_store.cpp — implementation of the log-structured blob store.
//
// Single-segment design: all records live in one append-only file. The opaque
// handle (segment, length, offset) encodes the record's location directly, so a
// load is an O(1) pread with no index lookup. segment is always 1; it is kept in
// the handle only so the serialized format is stable.
#include "blob_store.h"

#include "crc32c.h"
#include "lru_cache.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace bs {
namespace {

constexpr uint32_t kMagic = 0x424C4F42;  // 'BLOB'
constexpr uint64_t kHeaderSize = 12;     // magic(4) + len(4) + crc(4)
constexpr uint64_t kFooterSize = 4;      // len(4)
constexpr uint32_t kSegmentId  = 1;      // the one and only segment
constexpr uint64_t kMaxSegmentSize = 0xFFFFFFFFull;  // offsets must fit a uint32 cache key

void put_u32(char* p, uint32_t v) {
    p[0] = char(v & 0xFF); p[1] = char((v >> 8) & 0xFF);
    p[2] = char((v >> 16) & 0xFF); p[3] = char((v >> 24) & 0xFF);
}
uint32_t get_u32(const char* p) {
    return uint32_t(uint8_t(p[0])) | (uint32_t(uint8_t(p[1])) << 8) |
           (uint32_t(uint8_t(p[2])) << 16) | (uint32_t(uint8_t(p[3])) << 24);
}

void pwrite_all(int fd, const char* buf, size_t n, uint64_t off) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = ::pwrite(fd, buf + done, n - done, (off_t)(off + done));
        if (w < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "pwrite");
        }
        done += (size_t)w;
    }
}

// Returns bytes read; a short read (EOF) returns < n rather than throwing.
size_t pread_all(int fd, char* buf, size_t n, uint64_t off) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::pread(fd, buf + done, n - done, (off_t)(off + done));
        if (r < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "pread");
        }
        if (r == 0) break;  // EOF
        done += (size_t)r;
    }
    return done;
}

std::string segment_path(const std::string& dir) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06u.seg", kSegmentId);
    return dir + "/" + name;
}

uint64_t cache_key(const Index& idx) {
    return (uint64_t(idx.segment) << 32) | uint32_t(idx.offset);
}

// Frame one record at dst: [magic|len|crc][payload|footer=len].
// The CRC covers the header (magic+len) AND the payload, so a flip anywhere in
// the framing — not just the payload — is caught at load(). Returns total bytes.
uint64_t frame_record(char* dst, const void* data, size_t len) {
    put_u32(dst + 0, kMagic);
    put_u32(dst + 4, (uint32_t)len);
    uint32_t crc = crc32c(dst, 8);             // cover magic + len
    crc = crc32c(data, len, crc);              // cover payload
    put_u32(dst + 8, crc);
    if (len) std::memcpy(dst + kHeaderSize, data, len);
    put_u32(dst + kHeaderSize + len, (uint32_t)len);  // footer (torn-write delimiter)
    return kHeaderSize + len + kFooterSize;
}

}  // namespace

// ---- Index serialization ---------------------------------------------------

std::array<uint8_t, 16> Index::bytes() const {
    std::array<uint8_t, 16> b{};
    char* p = reinterpret_cast<char*>(b.data());
    put_u32(p + 0, segment);
    put_u32(p + 4, length);
    put_u32(p + 8, uint32_t(offset & 0xFFFFFFFF));
    put_u32(p + 12, uint32_t(offset >> 32));
    return b;
}

Index Index::from_bytes(const std::array<uint8_t, 16>& b) {
    const char* p = reinterpret_cast<const char*>(b.data());
    Index i;
    i.segment = get_u32(p + 0);
    i.length = get_u32(p + 4);
    i.offset = uint64_t(get_u32(p + 8)) | (uint64_t(get_u32(p + 12)) << 32);
    return i;
}

// ---- Impl ------------------------------------------------------------------

struct BlobStore::Impl {
    Options opts;
    int dir_fd = -1;

    // The single segment file and its append point.
    std::mutex append_mu;
    int seg_fd = -1;
    uint64_t cursor = 0;

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

    std::unique_ptr<ShardedLruCache> cache;

    // Stats.
    std::atomic<uint64_t> n_stores{0}, n_loads{0}, n_bytes{0};
    std::atomic<uint64_t> n_hits{0}, n_misses{0}, n_coalesced{0}, n_fsyncs{0};

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

    // Scan the segment from offset 0 to find the end of the last *framing-sound*
    // record. A record is framing-sound if its magic matches, it fits within the
    // file, and the header length and footer length agree. The first record that
    // fails any of those is a torn tail (a crash mid-append) and everything from
    // there on is truncated.
    //
    // A record whose framing is sound but whose CRC fails is interior bit-rot, not
    // a torn tail: it is left in place (load() rejects it) and the scan continues,
    // so corruption of one record never discards the records written after it.
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

    struct Reservation { int fd; uint64_t off; };

    // Reserve a contiguous region for `total` bytes at the append point.
    Reservation reserve(uint64_t total) {
        std::lock_guard<std::mutex> lk(append_mu);
        if (cursor + total > opts.segment_size)
            throw std::runtime_error("store is full (segment_size exceeded)");
        Reservation r{seg_fd, cursor};
        cursor += total;
        return r;
    }

    // Make a just-written region (of `bytes` payload) durable per the configured mode.
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
                // Return immediately; just nudge the background syncer if we've piled
                // up enough unsynced data to cross the size threshold.
                uint64_t u = unsynced_bytes.fetch_add(bytes) + bytes;
                if (u >= opts.async_flush_bytes) commit_cv.notify_one();
                break;
            }
            case Durability::OsBuffered:
                break;
        }
    }

    // One syncer serves both background modes. GroupCommit drives it via queued
    // waiters (flush as soon as one appears); AsyncFlush drives it via the
    // size-or-interval threshold and signals no one.
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
                batch.swap(pending);              // size threshold OR ...
                bytes = unsynced_bytes.exchange(0);  // ... interval timeout flushes the tail
            }
            if (batch.empty() && bytes == 0) continue;
            flush_batch(batch);
        }
        // Drain on shutdown (clean close also fsyncs again via sync()).
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

    // Read and fully validate the record addressed by idx.
    std::shared_ptr<const std::string> read_record(const Index& idx) {
        if (idx.segment != kSegmentId) throw std::runtime_error("unknown segment in index");
        const uint64_t total = kHeaderSize + idx.length + kFooterSize;
        std::string buf(total, '\0');
        if (pread_all(seg_fd, buf.data(), total, idx.offset) != total)
            throw std::runtime_error("index out of range");
        if (get_u32(buf.data()) != kMagic) throw std::runtime_error("bad magic");
        uint32_t len = get_u32(buf.data() + 4);
        if (len != idx.length) throw std::runtime_error("length mismatch");
        uint32_t want = get_u32(buf.data() + 8);
        uint32_t crc = crc32c(buf.data(), 8);                  // header (magic+len)
        crc = crc32c(buf.data() + kHeaderSize, len, crc);      // payload
        if (crc != want) throw std::runtime_error("crc mismatch");
        if (get_u32(buf.data() + kHeaderSize + len) != len) throw std::runtime_error("footer mismatch");
        return std::make_shared<const std::string>(buf.substr(kHeaderSize, len));
    }
};

// ---- BlobStore public methods ----------------------------------------------

BlobStore::BlobStore() : p_(std::make_unique<Impl>()) {}
BlobStore::~BlobStore() {
    if (!p_) return;
    p_->stop_syncer();
    sync();
    if (p_->seg_fd >= 0) ::close(p_->seg_fd);
    if (p_->dir_fd >= 0) ::close(p_->dir_fd);
}

std::unique_ptr<BlobStore> BlobStore::open(const Options& opts) {
    if (opts.segment_size == 0 || opts.segment_size > kMaxSegmentSize)
        throw std::invalid_argument("segment_size must be in (0, 4GiB)");
    std::unique_ptr<BlobStore> store(new BlobStore());
    store->p_->opts = opts;
    if (opts.enable_cache)
        store->p_->cache = std::make_unique<ShardedLruCache>(opts.cache_capacity_bytes, opts.cache_shards);
    store->p_->open_existing_or_create();
    store->p_->start_syncer();
    return store;
}

Index BlobStore::store(const void* data, size_t len) {
    if (len > 0xFFFFFFFFull) throw std::invalid_argument("blob too large (>4GiB)");
    Impl& s = *p_;

    const uint64_t total = kHeaderSize + len + kFooterSize;
    std::string buf(total, '\0');
    frame_record(buf.data(), data, len);

    auto r = s.reserve(total);
    pwrite_all(r.fd, buf.data(), total, r.off);
    s.commit(total);

    s.n_stores.fetch_add(1);
    s.n_bytes.fetch_add(len);
    return Index{kSegmentId, (uint32_t)len, r.off};
}

std::vector<Index> BlobStore::appendBatch(const std::vector<std::string_view>& blobs) {
    if (blobs.empty()) return {};
    Impl& s = *p_;

    uint64_t total = 0;
    for (auto b : blobs) {
        if (b.size() > 0xFFFFFFFFull) throw std::invalid_argument("blob too large (>4GiB)");
        total += kHeaderSize + b.size() + kFooterSize;
    }

    // Frame the whole batch into one contiguous buffer, then write + commit once.
    std::string buf(total, '\0');
    uint64_t p = 0;
    for (auto b : blobs) p += frame_record(buf.data() + p, b.data(), b.size());

    auto r = s.reserve(total);
    pwrite_all(r.fd, buf.data(), total, r.off);
    s.commit(total);

    std::vector<Index> out;
    out.reserve(blobs.size());
    uint64_t off = r.off, bytes = 0;
    for (auto b : blobs) {
        out.push_back(Index{kSegmentId, (uint32_t)b.size(), off});
        off += kHeaderSize + b.size() + kFooterSize;
        bytes += b.size();
    }
    s.n_stores.fetch_add(blobs.size());
    s.n_bytes.fetch_add(bytes);
    return out;
}

std::shared_ptr<const std::string> BlobStore::load(Index idx) {
    Impl& s = *p_;
    s.n_loads.fetch_add(1);
    if (!idx.valid()) throw std::runtime_error("invalid index");

    if (!s.cache) return s.read_record(idx);

    ShardedLruCache::Outcome o;
    auto val = s.cache->get_or_load(cache_key(idx), [&] { return s.read_record(idx); }, &o);
    if (o.hit) s.n_hits.fetch_add(1); else s.n_misses.fetch_add(1);
    if (o.coalesced) s.n_coalesced.fetch_add(1);
    return val;
}

std::vector<std::shared_ptr<const std::string>> BlobStore::loadBatch(const std::vector<Index>& idxs) {
    std::vector<std::shared_ptr<const std::string>> out;
    out.reserve(idxs.size());
    for (const auto& idx : idxs) out.push_back(load(idx));
    return out;
}

void BlobStore::sync() {
    Impl& s = *p_;
    if (s.seg_fd >= 0) { ::fsync(s.seg_fd); s.n_fsyncs.fetch_add(1); }
}

void BlobStore::evict_os_cache() {
    Impl& s = *p_;
    if (s.seg_fd < 0) return;
    ::fsync(s.seg_fd);  // DONTNEED only evicts written-back pages; flush first
    ::posix_fadvise(s.seg_fd, 0, 0, POSIX_FADV_DONTNEED);
}

BlobStore::Stats BlobStore::stats() const {
    Impl& s = *p_;
    Stats st;
    st.stores = s.n_stores.load();
    st.loads = s.n_loads.load();
    st.bytes_written = s.n_bytes.load();
    st.cache_hits = s.n_hits.load();
    st.cache_misses = s.n_misses.load();
    st.coalesced = s.n_coalesced.load();
    st.fsyncs = s.n_fsyncs.load();
    st.segments = 1;
    return st;
}

}  // namespace bs
