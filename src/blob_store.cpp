// blob_store.cpp — implementation of the log-structured blob store.
#include "blob_store.h"

#include "crc32c.h"
#include "lru_cache.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace bs {
namespace {

constexpr uint32_t kMagic = 0x424C4F42;  // 'BLOB'
constexpr uint64_t kHeaderSize = 12;     // magic(4) + len(4) + crc(4)
constexpr uint64_t kFooterSize = 4;      // len(4)
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

std::string segment_path(const std::string& dir, uint32_t id) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06u.seg", id);
    return dir + "/" + name;
}

uint64_t cache_key(const Index& idx) {
    return (uint64_t(idx.segment) << 32) | uint32_t(idx.offset);
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

    // Segment fd registry (read-mostly: only grows on rotation).
    mutable std::shared_mutex reg_mu;
    std::unordered_map<uint32_t, int> fds;

    // Active append point.
    std::mutex append_mu;
    uint32_t active_id = 0;
    int active_fd = -1;
    uint64_t cursor = 0;

    // Group-commit syncer.
    struct Waiter {
        uint32_t seg;
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        void wait() { std::unique_lock<std::mutex> l(m); cv.wait(l, [&] { return done; }); }
        void signal() { { std::lock_guard<std::mutex> l(m); done = true; } cv.notify_one(); }
    };
    std::mutex commit_mu;
    std::condition_variable commit_cv;
    std::vector<Waiter*> pending;
    std::thread syncer;
    std::atomic<bool> running{false};

    std::unique_ptr<ShardedLruCache> cache;

    // Stats.
    std::atomic<uint64_t> n_stores{0}, n_loads{0}, n_bytes{0};
    std::atomic<uint64_t> n_hits{0}, n_misses{0}, n_coalesced{0}, n_fsyncs{0};
    std::atomic<uint32_t> n_segments{0};

    int fd_for(uint32_t seg) const {
        std::shared_lock<std::shared_mutex> lk(reg_mu);
        auto it = fds.find(seg);
        return it == fds.end() ? -1 : it->second;
    }

    void fsync_dir() { if (dir_fd >= 0) ::fsync(dir_fd); }

    void open_existing_or_create() {
        namespace fs = std::filesystem;
        fs::create_directories(opts.dir);
        dir_fd = ::open(opts.dir.c_str(), O_RDONLY | O_DIRECTORY);

        std::vector<uint32_t> ids;
        for (auto& e : fs::directory_iterator(opts.dir)) {
            auto name = e.path().filename().string();
            if (name.size() == 10 && name.substr(6) == ".seg") {
                try { ids.push_back((uint32_t)std::stoul(name.substr(0, 6))); }
                catch (...) {}
            }
        }
        std::sort(ids.begin(), ids.end());

        if (ids.empty()) {
            create_segment(1);
            active_id = 1;
            cursor = 0;
            return;
        }
        for (uint32_t id : ids) {
            int fd = ::open(segment_path(opts.dir, id).c_str(), O_RDWR);
            if (fd < 0) throw std::system_error(errno, std::generic_category(), "open seg");
            std::unique_lock<std::shared_mutex> lk(reg_mu);
            fds[id] = fd;
        }
        n_segments.store((uint32_t)ids.size());
        active_id = ids.back();
        active_fd = fd_for(active_id);
        cursor = recover_tail(active_fd);
    }

    void create_segment(uint32_t id) {
        int fd = ::open(segment_path(opts.dir, id).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::system_error(errno, std::generic_category(), "create seg");
        {
            std::unique_lock<std::shared_mutex> lk(reg_mu);
            fds[id] = fd;
        }
        active_fd = fd;
        n_segments.fetch_add(1);
        fsync_dir();  // make the new file's directory entry durable
    }

    // Scan a segment from offset 0, returning the end of the last intact record.
    // The first record that fails framing/crc/bounds marks a torn tail.
    uint64_t recover_tail(int fd) {
        struct stat st{};
        if (::fstat(fd, &st) != 0) return 0;
        uint64_t size = (uint64_t)st.st_size;
        uint64_t pos = 0;
        std::string hdr(kHeaderSize, '\0');
        while (pos + kHeaderSize <= size) {
            if (pread_all(fd, hdr.data(), kHeaderSize, pos) != kHeaderSize) break;
            uint32_t magic = get_u32(hdr.data());
            if (magic != kMagic) break;
            uint32_t len = get_u32(hdr.data() + 4);
            uint32_t crc = get_u32(hdr.data() + 8);
            uint64_t reclen = kHeaderSize + len + kFooterSize;
            if (pos + reclen > size) break;  // torn: record runs past EOF
            std::string body(len + kFooterSize, '\0');
            if (pread_all(fd, body.data(), body.size(), pos + kHeaderSize) != body.size()) break;
            if (crc32c(body.data(), len) != crc) break;
            if (get_u32(body.data() + len) != len) break;  // footer mismatch
            pos += reclen;
        }
        if (pos < size) ::ftruncate(fd, (off_t)pos);  // drop the torn tail
        return pos;
    }

    struct Reservation { uint32_t seg; int fd; uint64_t off; };

    Reservation reserve(uint64_t total) {
        std::lock_guard<std::mutex> lk(append_mu);
        // Rotate only if the current segment already holds data; a fresh segment
        // always accepts the record (supports records larger than segment_size).
        if (cursor > 0 && cursor + total > opts.segment_size) {
            create_segment(active_id + 1);
            active_id += 1;
            cursor = 0;
        }
        Reservation r{active_id, active_fd, cursor};
        cursor += total;
        return r;
    }

    void syncer_loop() {
        while (running.load()) {
            std::vector<Waiter*> batch;
            {
                std::unique_lock<std::mutex> lk(commit_mu);
                commit_cv.wait_for(lk, std::chrono::microseconds(opts.group_commit_interval_us),
                                   [&] { return !pending.empty() || !running.load(); });
                batch.swap(pending);
            }
            if (batch.empty()) continue;
            flush_batch(batch);
        }
        // Drain any stragglers on shutdown.
        std::vector<Waiter*> batch;
        { std::lock_guard<std::mutex> lk(commit_mu); batch.swap(pending); }
        if (!batch.empty()) flush_batch(batch);
    }

    void flush_batch(std::vector<Waiter*>& batch) {
        std::set<uint32_t> segs;
        for (auto* w : batch) segs.insert(w->seg);
        for (uint32_t s : segs) {
            int fd = fd_for(s);
            if (fd >= 0) { ::fsync(fd); n_fsyncs.fetch_add(1); }
        }
        for (auto* w : batch) w->signal();
    }

    void start_syncer() {
        if (opts.durability == Durability::GroupCommit) {
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
};

// ---- BlobStore public methods ----------------------------------------------

BlobStore::BlobStore() : p_(std::make_unique<Impl>()) {}
BlobStore::~BlobStore() {
    if (!p_) return;
    p_->stop_syncer();
    sync();
    std::unique_lock<std::shared_mutex> lk(p_->reg_mu);
    for (auto& [id, fd] : p_->fds) ::close(fd);
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
    put_u32(buf.data() + 0, kMagic);
    put_u32(buf.data() + 4, (uint32_t)len);
    uint32_t crc = crc32c(data, len);
    put_u32(buf.data() + 8, crc);
    if (len) std::memcpy(buf.data() + kHeaderSize, data, len);
    put_u32(buf.data() + kHeaderSize + len, (uint32_t)len);  // footer

    auto r = s.reserve(total);
    pwrite_all(r.fd, buf.data(), total, r.off);

    switch (s.opts.durability) {
        case Durability::GroupCommit: {
            Impl::Waiter w; w.seg = r.seg;
            { std::lock_guard<std::mutex> lk(s.commit_mu); s.pending.push_back(&w); }
            s.commit_cv.notify_one();
            w.wait();
            break;
        }
        case Durability::Sync:
            ::fsync(r.fd);
            s.n_fsyncs.fetch_add(1);
            break;
        case Durability::OsBuffered:
            break;
    }

    s.n_stores.fetch_add(1);
    s.n_bytes.fetch_add(len);
    return Index{r.seg, (uint32_t)len, r.off};
}

std::shared_ptr<const std::string> BlobStore::load(Index idx) {
    Impl& s = *p_;
    s.n_loads.fetch_add(1);
    if (!idx.valid()) throw std::runtime_error("invalid index");

    auto read_record = [&]() -> std::shared_ptr<const std::string> {
        int fd = s.fd_for(idx.segment);
        if (fd < 0) throw std::runtime_error("unknown segment in index");
        const uint64_t total = kHeaderSize + idx.length + kFooterSize;
        std::string buf(total, '\0');
        if (pread_all(fd, buf.data(), total, idx.offset) != total)
            throw std::runtime_error("index out of range");
        if (get_u32(buf.data()) != kMagic) throw std::runtime_error("bad magic");
        uint32_t len = get_u32(buf.data() + 4);
        uint32_t crc = get_u32(buf.data() + 8);
        if (len != idx.length) throw std::runtime_error("length mismatch");
        if (crc32c(buf.data() + kHeaderSize, len) != crc) throw std::runtime_error("crc mismatch");
        if (get_u32(buf.data() + kHeaderSize + len) != len) throw std::runtime_error("footer mismatch");
        return std::make_shared<const std::string>(buf.substr(kHeaderSize, len));
    };

    if (!s.cache) return read_record();

    ShardedLruCache::Outcome o;
    auto val = s.cache->get_or_load(cache_key(idx), read_record, &o);
    if (o.hit) s.n_hits.fetch_add(1); else s.n_misses.fetch_add(1);
    if (o.coalesced) s.n_coalesced.fetch_add(1);
    return val;
}

void BlobStore::sync() {
    Impl& s = *p_;
    std::shared_lock<std::shared_mutex> lk(s.reg_mu);
    for (auto& [id, fd] : s.fds) { ::fsync(fd); s.n_fsyncs.fetch_add(1); }
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
    st.segments = s.n_segments.load();
    return st;
}

}  // namespace bs
