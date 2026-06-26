// Spec: the RecordWriter/RecordReader contract — append/load roundtrip, arbitrary
// length, handle serialization & opacity, batch append/load, forward scan, the
// durability knobs, and bad-handle rejection. Writer and reader are separate
// objects sharing only the file on disk.
#include "record_reader.h"
#include "record_writer.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

using namespace bs;

// Path of the single segment file (mirrors the store's "%06u.seg" naming, id=1).
static std::string seg_path(const std::string& dir) { return dir + "/000001.seg"; }

// A unique scratch dir per test; removed on construction so each run is clean.
struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_wr_") + name;
        (void)std::system(("rm -rf '" + path + "'").c_str());
    }
    ~TmpDir() { (void)std::system(("rm -rf '" + path + "'").c_str()); }
};

static std::string rand_blob(std::mt19937_64& rng, size_t n) {
    std::string s(n, '\0');
    for (auto& c : s) c = char(rng() & 0xFF);
    return s;
}

TEST(WriterReader, AppendThenLoadRoundtripsBytes) {
    TmpDir d("roundtrip");
    auto w = RecordWriter::open({.dir = d.path});
    auto idx = w->append("hello world");
    EXPECT_TRUE(idx.valid());
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(idx), std::string("hello world"));
}

TEST(WriterReader, DistinctAppendsGetDistinctHandlesAndValues) {
    TmpDir d("distinct");
    auto w = RecordWriter::open({.dir = d.path});
    auto a = w->append("aaa");
    auto b = w->append("bbbb");
    EXPECT_NE(a, b);
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(a), std::string("aaa"));
    EXPECT_EQ(r->load(b), std::string("bbbb"));
}

TEST(WriterReader, ArbitraryLengths) {
    TmpDir d("arbitrary");
    auto w = RecordWriter::open({.dir = d.path});
    std::mt19937_64 rng(123);
    std::vector<size_t> sizes = {0, 1, 7, 4096, 4097, 1u << 20, (1u << 20) + 333};
    std::vector<std::pair<Index, std::string>> kept;
    for (size_t n : sizes) {
        auto blob = rand_blob(rng, n);
        kept.push_back({w->append(blob), blob});
    }
    auto r = RecordReader::open({.dir = d.path});
    for (auto& [idx, blob] : kept) {
        EXPECT_EQ(idx.length, (uint32_t)blob.size());
        EXPECT_EQ(r->load(idx), blob);
    }
}

TEST(WriterReader, IndexSerializesTo16BytesAndBack) {
    Index a{.segment = 3, .length = 4096, .offset = 123456789};
    auto bytes = a.bytes();
    EXPECT_EQ(bytes.size(), (size_t)16);
    EXPECT_EQ(Index::from_bytes(bytes), a);
}

TEST(WriterReader, DefaultIndexIsInvalid) {
    Index z;
    EXPECT_FALSE(z.valid());
}

TEST(WriterReader, HandlesSurviveCloseReopen) {
    TmpDir d("reopen");
    std::array<uint8_t, 16> saved{};
    {
        auto w = RecordWriter::open({.dir = d.path});
        saved = w->append("persisted").bytes();
        w->close();
    }
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(Index::from_bytes(saved)), std::string("persisted"));
}

TEST(WriterReader, LoadRejectsCorruptOrOutOfRangeHandle) {
    TmpDir d("badhandle");
    auto w = RecordWriter::open({.dir = d.path});
    w->append("real");
    w->sync();
    auto r = RecordReader::open({.dir = d.path});
    Index bogus{.segment = 1, .length = 10, .offset = 99999999};  // past EOF
    EXPECT_ANY_THROW(r->load(bogus));
}

TEST(WriterReader, WriterCountsAppendsReaderCountsLoads) {
    TmpDir d("stats");
    auto w = RecordWriter::open({.dir = d.path});
    auto idx = w->append("x");
    w->sync();
    EXPECT_EQ(w->stats().appends, (uint64_t)1);
    EXPECT_EQ(w->stats().segments, (uint32_t)1);

    auto r = RecordReader::open({.dir = d.path});
    r->load(idx);
    r->load(idx);
    EXPECT_EQ(r->stats().loads, (uint64_t)2);
}

TEST(WriterReader, AppendThrowsWhenSegmentCapExceeded) {
    TmpDir d("full");
    Options opt{.dir = d.path};
    opt.segment_size = 4096;  // tiny cap
    auto w = RecordWriter::open(opt);
    w->append(std::string(2000, 'x'));                 // fits
    EXPECT_ANY_THROW(w->append(std::string(4000, 'y')));  // would overflow the cap
}

TEST(WriterReader, AppendAfterCloseThrows) {
    TmpDir d("afterclose");
    auto w = RecordWriter::open({.dir = d.path});
    w->append("ok");
    w->close();
    EXPECT_ANY_THROW(w->append("nope"));
    w->close();  // idempotent
}

TEST(WriterReader, AppendBatchReturnsOneHandlePerBlob) {
    TmpDir d("batch");
    auto w = RecordWriter::open({.dir = d.path});
    std::vector<std::string> values = {"", "a", std::string(4096, 'Z'), "tail",
                                       std::string(1234, '\x7F')};
    auto idxs = w->appendBatch(values);
    ASSERT_EQ(idxs.size(), values.size());
    w->sync();
    auto r = RecordReader::open({.dir = d.path});
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(idxs[i].length, (uint32_t)values[i].size());
        EXPECT_EQ(r->load(idxs[i]), values[i]);
    }
    EXPECT_EQ(w->stats().appends, (uint64_t)values.size());
    EXPECT_TRUE(w->appendBatch({}).empty());  // empty batch is a no-op
}

TEST(WriterReader, LoadBatchReturnsValuesInOrderSurvivesReopen) {
    TmpDir d("loadbatch");
    std::vector<std::array<uint8_t, 16>> saved;
    std::vector<std::string> values = {"one", "two", "three", std::string(5000, 'Q')};
    {
        auto w = RecordWriter::open({.dir = d.path});
        for (auto& idx : w->appendBatch(values)) saved.push_back(idx.bytes());
        w->close();
    }
    auto r = RecordReader::open({.dir = d.path});
    std::vector<Index> idxs;
    for (auto& b : saved) idxs.push_back(Index::from_bytes(b));
    auto vals = r->loadBatch(idxs);
    ASSERT_EQ(vals.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) EXPECT_EQ(vals[i], values[i]);
}

TEST(WriterReader, ForwardScanYieldsEveryRecordInWriteOrder) {
    TmpDir d("iter");
    auto w = RecordWriter::open({.dir = d.path});
    std::mt19937_64 rng(77);
    std::vector<std::pair<Index, std::string>> expect;
    for (int i = 0; i < 60; ++i) {
        auto blob = rand_blob(rng, rng() % 5000);  // includes occasional empty
        expect.push_back({w->append(blob), blob});
    }
    w->sync();
    auto r = RecordReader::open({.dir = d.path});
    size_t i = 0;
    for (auto& rec : r->scan()) {
        ASSERT_LT(i, expect.size());
        EXPECT_EQ(rec.index, expect[i].first);
        EXPECT_EQ(rec.value, expect[i].second);
        ++i;
    }
    EXPECT_EQ(i, expect.size());
}

TEST(WriterReader, ForwardScanOnEmptyStoreYieldsNothing) {
    TmpDir d("iterempty");
    {
        auto w = RecordWriter::open({.dir = d.path});
        auto r0 = RecordReader::open({.dir = d.path});
        size_t n = 0;
        for (auto& rec : r0->scan()) { (void)rec; ++n; }
        EXPECT_EQ(n, (size_t)0);
        w->append("a");
        w->append("bb");
        w->close();
    }
    auto r = RecordReader::open({.dir = d.path});
    std::vector<std::string> got;
    for (auto& rec : r->scan()) got.push_back(rec.value);
    ASSERT_EQ(got.size(), (size_t)2);
    EXPECT_EQ(got[0], std::string("a"));
    EXPECT_EQ(got[1], std::string("bb"));
}

TEST(WriterReader, ForwardScanSkipsCrcCorruptRecord) {
    TmpDir d("iterskip");
    std::vector<Index> h;
    std::vector<std::string> payloads;
    {
        auto w = RecordWriter::open({.dir = d.path});
        for (int i = 0; i < 5; ++i) {
            std::string p(200, char('A' + i));
            payloads.push_back(p);
            h.push_back(w->append(p));
        }
        w->close();
    }
    // Corrupt the payload of record 2 (framing stays sound => scan skips it).
    int fd = ::open(seg_path(d.path).c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    uint64_t off = h[2].offset + 12 + h[2].length / 2;
    char b = 0;
    ASSERT_EQ(::pread(fd, &b, 1, (off_t)off), 1);
    b ^= 0x10;
    ASSERT_EQ(::pwrite(fd, &b, 1, (off_t)off), 1);
    ::close(fd);

    auto r = RecordReader::open({.dir = d.path});
    std::vector<std::string> got;
    for (auto& rec : r->scan()) got.push_back(rec.value);
    ASSERT_EQ(got.size(), (size_t)4);
    EXPECT_EQ(got[0], payloads[0]);
    EXPECT_EQ(got[1], payloads[1]);
    EXPECT_EQ(got[2], payloads[3]);
    EXPECT_EQ(got[3], payloads[4]);
}

TEST(WriterReader, EvictOsCacheDropsPagesWithoutAffectingCorrectness) {
    TmpDir d("evict");
    auto w = RecordWriter::open({.dir = d.path});
    std::mt19937_64 rng(55);
    std::vector<std::pair<Index, std::string>> kept;
    for (int i = 0; i < 50; ++i) {
        auto blob = rand_blob(rng, 1 + rng() % 9000);
        kept.push_back({w->append(blob), blob});
    }
    w->sync();
    auto r = RecordReader::open({.dir = d.path});
    r->evict_os_cache();
    for (auto& [idx, blob] : kept) EXPECT_EQ(r->load(idx), blob);
    r->evict_os_cache();  // idempotent
    EXPECT_EQ(r->load(kept[0].first), kept[0].second);
}

TEST(WriterReader, AsyncFlushBackgroundFsyncsOnSizeThreshold) {
    TmpDir d("async");
    std::array<uint8_t, 16> saved{};
    {
        Options opt{.dir = d.path};
        opt.durability = Durability::AsyncFlush;
        opt.async_flush_bytes = 64 * 1024;      // small size trigger
        opt.async_flush_interval_us = 100000;   // long interval => size trigger fires first
        auto w = RecordWriter::open(opt);
        for (int i = 0; i < 200; ++i) saved = w->append(std::string(1024, 'a')).bytes();

        bool flushed = false;
        for (int i = 0; i < 100 && !flushed; ++i) {
            if (w->stats().fsyncs > 0) flushed = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_TRUE(flushed);
        w->close();
    }
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(Index::from_bytes(saved)), std::string(1024, 'a'));
}
