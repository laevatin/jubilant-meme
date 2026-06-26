// Spec: crash recovery. Recovery is the *writer's* job: opening a RecordWriter
// scans the segment, truncates any torn tail, and rederives the cursor. A reader
// then loads the surviving records (and rejects corrupt ones via CRC). These tests
// inject on-disk faults into a closed store, reopen a writer to recover, and check
// what survives.
#include "record_reader.h"
#include "record_writer.h"

#include <gtest/gtest.h>

#include <array>
#include <random>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace bs;

struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_rec_") + name;
        (void)std::system(("rm -rf '" + path + "'").c_str());
    }
    ~TmpDir() { (void)std::system(("rm -rf '" + path + "'").c_str()); }
};

// The single segment file (id=1, "%06u.seg").
static std::string seg_path(const std::string& dir) { return dir + "/000001.seg"; }
static uint64_t file_size(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0 ? (uint64_t)st.st_size : 0;
}

TEST(Recovery, TornTailIsTruncatedPriorRecordsSurvive) {
    TmpDir d("torn");
    std::vector<std::array<uint8_t, 16>> handles;
    {
        auto w = RecordWriter::open({.dir = d.path});
        for (int i = 0; i < 20; ++i)
            handles.push_back(w->append("record-" + std::to_string(i)).bytes());
        w->close();
    }

    // Simulate a crash mid-append: garbage bytes appended with no valid framing.
    int fd = ::open(seg_path(d.path).c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    std::string junk(37, '\xAB');
    ASSERT_EQ(::write(fd, junk.data(), junk.size()), (ssize_t)junk.size());
    ::close(fd);

    // Reopen a writer: recovery truncates the junk; new writes still work.
    auto w = RecordWriter::open({.dir = d.path});
    auto post = w->append("after-recovery");
    w->sync();

    auto r = RecordReader::open({.dir = d.path});
    for (auto& h : handles) {
        auto got = r->load(Index::from_bytes(h));
        EXPECT_EQ(got.rfind("record-", 0), (size_t)0);
    }
    EXPECT_EQ(r->load(post), std::string("after-recovery"));
}

TEST(Recovery, EmptyOrNewDirectoryYieldsUsableStore) {
    TmpDir d("fresh");
    auto w = RecordWriter::open({.dir = d.path});
    auto idx = w->append("first");
    w->sync();
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(idx), std::string("first"));
}

// In-scope error point: a single-bit flip inside a stored payload must be caught by
// CRC at load() (throw), while every other record — including the ones written
// *after* the victim — still loads. Exercises the recovery rule that interior
// bit-rot (framing intact, CRC bad) is left in place and stepped over.
TEST(Recovery, InteriorBitFlipCaughtAtLoadNeighboursSurvive) {
    TmpDir d("bitflip");
    std::mt19937_64 rng(1);
    std::vector<Index> h;
    std::vector<std::string> payloads;
    {
        auto w = RecordWriter::open({.dir = d.path});
        for (int i = 0; i < 40; ++i) {
            std::string p(1000, '\0');
            for (auto& c : p) c = char(rng() & 0xFF);
            payloads.push_back(p);
            h.push_back(w->append(p));
        }
        w->close();
    }
    uint64_t before = file_size(seg_path(d.path));

    Index victim = h[10];
    int fd = ::open(seg_path(d.path).c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    uint64_t off = victim.offset + 12 + victim.length / 2;
    char byte = 0;
    ASSERT_EQ(::pread(fd, &byte, 1, (off_t)off), 1);
    byte ^= 0x08;
    ASSERT_EQ(::pwrite(fd, &byte, 1, (off_t)off), 1);
    ::close(fd);

    // Writer recovery must NOT truncate a framing-sound-but-CRC-bad interior record.
    auto w = RecordWriter::open({.dir = d.path});
    EXPECT_EQ(file_size(seg_path(d.path)), before);
    w->close();

    auto r = RecordReader::open({.dir = d.path});
    EXPECT_ANY_THROW(r->load(victim));         // corrupted record rejected
    EXPECT_EQ(r->load(h[9]), payloads[9]);     // record before the victim survives
    EXPECT_EQ(r->load(h[11]), payloads[11]);   // record after the victim survives
    EXPECT_EQ(r->load(h[39]), payloads[39]);   // last record survives
}

// In-scope error point: a flip in the on-disk length field (now CRC-covered) makes
// the header/footer length copies disagree, so recovery treats it as a torn tail.
TEST(Recovery, FlipInLengthFieldIsCaught) {
    TmpDir d("lenflip");
    Index a, b;
    {
        auto w = RecordWriter::open({.dir = d.path});
        a = w->append("first-record");
        b = w->append("second-record");
        w->close();
    }

    int fd = ::open(seg_path(d.path).c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    uint64_t off = b.offset + 5;  // inside the 4-byte length field
    char byte = 0;
    ASSERT_EQ(::pread(fd, &byte, 1, (off_t)off), 1);
    byte ^= 0x01;
    ASSERT_EQ(::pwrite(fd, &byte, 1, (off_t)off), 1);
    ::close(fd);

    auto w = RecordWriter::open({.dir = d.path});  // recovery truncates b as a torn tail
    w->close();
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(a), std::string("first-record"));  // prior record intact
    EXPECT_ANY_THROW(r->load(b));                         // corrupt length rejected
}

// In-scope error point: a crash mid-pwrite that leaves a valid header but a
// truncated payload. Recovery drops that torn record and keeps complete prior ones.
TEST(Recovery, TruncatedPayloadAtTailIsDropped) {
    TmpDir d("truncpayload");
    std::vector<Index> h;
    std::vector<std::string> payloads;
    {
        auto w = RecordWriter::open({.dir = d.path});
        for (int i = 0; i < 20; ++i) {
            std::string p(500, char('A' + i));
            payloads.push_back(p);
            h.push_back(w->append(p));
        }
        w->close();
    }

    Index last = h.back();
    ::truncate(seg_path(d.path).c_str(), (off_t)(last.offset + 12 + last.length / 2));

    auto w = RecordWriter::open({.dir = d.path});
    auto fresh = w->append("post");
    w->sync();
    auto r = RecordReader::open({.dir = d.path});
    for (int i = 0; i < 19; ++i) EXPECT_EQ(r->load(h[i]), payloads[i]);
    EXPECT_ANY_THROW(r->load(last));  // torn record gone
    EXPECT_EQ(r->load(fresh), std::string("post"));
}

// In-scope error point: a crash that left only a partial record header (< 12 bytes)
// at the tail. The scan's header-bounds guard must stop cleanly and truncate exactly.
TEST(Recovery, PartialRecordHeaderAtTailIsDropped) {
    TmpDir d("partialhdr");
    std::vector<Index> h;
    {
        auto w = RecordWriter::open({.dir = d.path});
        for (int i = 0; i < 10; ++i) h.push_back(w->append("rec"));
        w->close();
    }

    Index last = h.back();
    ::truncate(seg_path(d.path).c_str(), (off_t)(last.offset + 5));  // 5 of 12 header bytes

    auto w = RecordWriter::open({.dir = d.path});  // recovery truncates to last good record
    w->close();
    EXPECT_EQ(file_size(seg_path(d.path)), last.offset);

    auto r = RecordReader::open({.dir = d.path});
    for (int i = 0; i < 9; ++i) EXPECT_EQ(r->load(h[i]), std::string("rec"));
    EXPECT_ANY_THROW(r->load(last));
}

// OsBuffered mode: after an explicit sync(), data must survive a clean reopen.
TEST(Recovery, OsBufferedAfterExplicitSyncSurvivesReopen) {
    TmpDir d("osbuffered");
    std::array<uint8_t, 16> saved{};
    {
        Options opt{.dir = d.path};
        opt.durability = Durability::OsBuffered;
        auto w = RecordWriter::open(opt);
        saved = w->append("buffered-then-synced").bytes();
        w->sync();
        w->close();
    }
    auto r = RecordReader::open({.dir = d.path});
    EXPECT_EQ(r->load(Index::from_bytes(saved)), std::string("buffered-then-synced"));
}
