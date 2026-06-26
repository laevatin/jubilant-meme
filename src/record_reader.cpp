// record_reader.cpp — load-by-Index and forward-scan, read-only.
#include "record_reader.h"

#include "record_format.h"

#include <atomic>
#include <iterator>
#include <ranges>
#include <stdexcept>

namespace bs {

using namespace fmt;

struct RecordReader::Impl {
    Options opts;
    int seg_fd = -1;
    ReadCache cache;
    std::atomic<uint64_t> n_loads{0};

    explicit Impl(const Options& o) : opts(o), cache(o) {}

    void open_for_read() {
        std::string path = segment_path(opts.dir);
        seg_fd = ::open(path.c_str(), O_RDONLY);
        if (seg_fd < 0) throw std::system_error(errno, std::generic_category(), "open segment (read)");
    }

    // Read and fully validate the record addressed by idx; return its payload.
    std::string read_record(const Index& idx) {
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
        return buf.substr(kHeaderSize, len);
    }
};

// ---- public methods --------------------------------------------------------

RecordReader::RecordReader() {}
RecordReader::~RecordReader() {
    if (p_ && p_->seg_fd >= 0) ::close(p_->seg_fd);
}

std::unique_ptr<RecordReader> RecordReader::open(const Options& opts) {
    std::unique_ptr<RecordReader> r(new RecordReader());
    r->p_ = std::make_unique<Impl>(opts);
    r->p_->open_for_read();
    return r;
}

std::string RecordReader::load(Index idx) {
    Impl& s = *p_;
    s.n_loads.fetch_add(1);
    if (!idx.valid()) throw std::runtime_error("invalid index");
    return s.cache.get_or_load(idx, [&] { return s.read_record(idx); });
}

std::vector<std::string> RecordReader::loadBatch(const std::vector<Index>& idxs) {
    std::vector<std::string> out;
    out.reserve(idxs.size());
    for (const auto& idx : idxs) out.push_back(load(idx));
    return out;
}

// ---- forward scan ----------------------------------------------------------

bool RecordReader::iter_step(uint64_t& off, uint64_t end, Record& out) {
    Impl& s = *p_;
    std::string hdr(kHeaderSize, '\0');
    for (;;) {
        if (off + kHeaderSize > end) return false;
        if (pread_all(s.seg_fd, hdr.data(), kHeaderSize, off) != kHeaderSize) return false;
        if (get_u32(hdr.data()) != kMagic) return false;            // framing gone -> end
        uint32_t len = get_u32(hdr.data() + 4);
        uint64_t reclen = kHeaderSize + len + kFooterSize;
        if (off + reclen > end) return false;                       // runs past snapshot -> end
        std::string body(len + kFooterSize, '\0');
        if (pread_all(s.seg_fd, body.data(), body.size(), off + kHeaderSize) != body.size())
            return false;
        if (get_u32(body.data() + len) != len) return false;        // len copies disagree -> end
        uint32_t want = get_u32(hdr.data() + 8);
        uint32_t crc = crc32c(hdr.data(), 8);              // header (magic+len)
        crc = crc32c(body.data(), len, crc);               // payload
        uint64_t rec_off = off;
        off += reclen;
        if (crc != want) continue;                                  // interior bit-rot -> skip
        out.index = Index{kSegmentId, len, rec_off};
        out.value = body.substr(0, len);
        return true;
    }
}

void RecordReader::Iterator::advance() {
    if (!owner_) { at_end_ = true; return; }
    at_end_ = !owner_->iter_step(off_, end_, cur_);
}

RecordReader::Scan RecordReader::scan() {
    Iterator it;
    it.owner_ = this;
    it.off_ = 0;
    struct stat st{};
    it.end_ = (::fstat(p_->seg_fd, &st) == 0) ? (uint64_t)st.st_size : 0;
    it.at_end_ = false;
    it.advance();  // position on the first record (or end, if empty)
    return Scan{it};
}

// The scan really is a C++20 input range over a single-pass input iterator
// terminated by std::default_sentinel. If either of these regresses, range-for and
// std::ranges algorithms over scan() would silently stop compiling — assert it here.
static_assert(std::input_iterator<RecordReader::Iterator>);
static_assert(std::sentinel_for<std::default_sentinel_t, RecordReader::Iterator>);
static_assert(std::ranges::input_range<RecordReader::Scan>);

void RecordReader::evict_os_cache() {
    Impl& s = *p_;
    if (s.seg_fd < 0) return;
    ::posix_fadvise(s.seg_fd, 0, 0, POSIX_FADV_DONTNEED);
}

RecordReader::Stats RecordReader::stats() const {
    Stats st;
    st.loads = p_->n_loads.load();
    return st;
}

}  // namespace bs
