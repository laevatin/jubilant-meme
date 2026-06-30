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

    // Read and fully validate the record addressed by idx; return its payload. The
    // handle carries len, so this reads the whole record in one pread, then shares
    // the framing checks (footer + CRC) with the scan/recovery paths via check_body.
    std::string read_record(const Index& idx) {
        if (idx.segment != kSegmentId) throw std::runtime_error("unknown segment in index");
        const uint64_t total = kHeaderSize + idx.length + kFooterSize;
        std::string buf(total, '\0');
        if (pread_all(seg_fd, buf.data(), total, idx.offset) != total)
            throw std::runtime_error("index out of range");
        if (get_u32(buf.data()) != kMagic) throw std::runtime_error("bad magic");
        if (get_u32(buf.data() + 4) != idx.length) throw std::runtime_error("length mismatch");
        const char* body = buf.data() + kHeaderSize;
        switch (check_body(buf.data(), idx.length, get_u32(buf.data() + 8), body)) {
            case BodyCheck::BadFooter: throw std::runtime_error("footer mismatch");
            case BodyCheck::BadCrc:    throw std::runtime_error("crc mismatch");
            case BodyCheck::Ok:        break;
        }
        return buf.substr(kHeaderSize, idx.length);
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
    for (;;) {
        FrameHeader h = read_header(s.seg_fd, off, end);
        if (!h.ok) return false;                                    // framing break -> end
        std::string body(h.len + kFooterSize, '\0');
        if (pread_all(s.seg_fd, body.data(), body.size(), off + kHeaderSize) != body.size())
            return false;                                           // short read -> end
        BodyCheck c = check_body(h.raw, h.len, h.crc, body.data());
        uint64_t rec_off = off;
        off += h.reclen;
        if (c == BodyCheck::BadFooter) return false;                // framing break -> end
        if (c == BodyCheck::BadCrc) continue;                       // interior bit-rot -> skip
        body.resize(h.len);                                         // drop the footer
        out.index = Index{kSegmentId, h.len, rec_off};
        out.value = std::move(body);
        return true;
    }
}

void RecordReader::Iterator::advance() {
    // Incrementing a past-the-end iterator is a no-op: it reads nothing and stays at
    // the end (rather than issuing a doomed pread or running past the snapshot).
    if (at_end_ || !owner_) { at_end_ = true; return; }
    at_end_ = !owner_->iter_step(off_, end_, cur_);
}

RecordReader::Scan RecordReader::scan() {
    Iterator it;
    it.owner_ = this;
    it.off_ = 0;
    it.end_ = file_size(p_->seg_fd);  // snapshot the scan boundary at scan() time
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
