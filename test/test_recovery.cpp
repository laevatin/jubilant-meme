// Spec: crash recovery. A torn write at the tail of the active segment must be
// truncated on reopen; all fully-written prior records must remain loadable, and
// the store must accept new writes afterward.
#include "blob_store.h"
#include "test_framework.h"

#include <cstdio>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using namespace bs;

struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name) {
        path = std::string("/tmp/blobstore_rec_") + name;
        (void)std::system(("rm -rf '" + path + "'").c_str());
    }
    ~TmpDir() { (void)std::system(("rm -rf '" + path + "'").c_str()); }
};

// The active segment is the highest-numbered NNNNNN.seg in the dir.
static std::string active_segment(const std::string& dir) {
    std::string cmd = "ls " + dir + "/*.seg 2>/dev/null | sort | tail -1";
    FILE* f = popen(cmd.c_str(), "r");
    char buf[4096] = {0};
    if (f) { if (fgets(buf, sizeof(buf), f)) {} pclose(f); }
    std::string s(buf);
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
    return s;
}

TEST("torn tail write is truncated; prior records survive") {
    TmpDir d("torn");
    std::vector<std::array<uint8_t, 16>> handles;
    {
        auto store = BlobStore::open({.dir = d.path});
        for (int i = 0; i < 20; ++i) {
            auto idx = store->store(std::string_view("record-" + std::to_string(i)));
            handles.push_back(idx.bytes());
        }
        store->sync();
    }

    // Simulate a crash mid-append: garbage bytes appended to the active segment.
    std::string seg = active_segment(d.path);
    CHECK(!seg.empty());
    int fd = ::open(seg.c_str(), O_WRONLY | O_APPEND);
    CHECK(fd >= 0);
    std::string junk(37, '\xAB');  // a partial/garbage record, no valid framing
    CHECK(::write(fd, junk.data(), junk.size()) == (ssize_t)junk.size());
    ::close(fd);

    // Reopen: recovery should truncate the junk and keep all 20 good records.
    auto store = BlobStore::open({.dir = d.path});
    for (auto& h : handles) {
        auto idx = Index::from_bytes(h);
        auto got = store->load(idx);
        CHECK(got->rfind("record-", 0) == 0);
    }
    // And new writes still work after recovery.
    auto idx = store->store(std::string_view("after-recovery"));
    CHECK_EQ(*store->load(idx), std::string("after-recovery"));
}

TEST("recovery from an empty/new directory yields a usable store") {
    TmpDir d("fresh");
    auto store = BlobStore::open({.dir = d.path});
    auto idx = store->store(std::string_view("first"));
    CHECK_EQ(*store->load(idx), std::string("first"));
}

// Path of a specific segment id (mirrors the store's "%06u.seg" naming).
static std::string seg_path(const std::string& dir, uint32_t id) {
    char b[32];
    std::snprintf(b, sizeof(b), "%06u.seg", id);
    return dir + "/" + b;
}
static uint64_t file_size(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0 ? (uint64_t)st.st_size : 0;
}

// In-scope error point: a single-bit flip inside a stored payload must be caught
// by CRC at load() time (throw), while neighbouring records still load. We flip a
// record in a *sealed* segment so recovery (active-only) leaves it in place and the
// detection happens purely via per-record CRC.
TEST("bit-flip in a stored payload is caught by CRC at load") {
    TmpDir d("bitflip");
    std::mt19937_64 rng(1);
    BlobStore::Options opt{.dir = d.path};
    opt.segment_size = 4096;  // ~3-4 records/segment => many sealed segments
    std::vector<Index> h;
    std::vector<std::string> payloads;
    {
        auto store = BlobStore::open(opt);
        for (int i = 0; i < 40; ++i) {
            std::string p(1000, '\0');
            for (auto& c : p) c = char(rng() & 0xFF);
            payloads.push_back(p);
            h.push_back(store->store(p));
        }
        store->sync();
    }
    Index victim = h[0];                  // in segment 1 (sealed)
    CHECK(victim.segment < h.back().segment);  // confirm it is sealed, not active
    // Flip one bit in the middle of the victim's payload (after the 12-byte header).
    std::string path = seg_path(d.path, victim.segment);
    int fd = ::open(path.c_str(), O_RDWR);
    CHECK(fd >= 0);
    uint64_t off = victim.offset + 12 + victim.length / 2;
    char byte = 0;
    CHECK(::pread(fd, &byte, 1, (off_t)off) == 1);
    byte ^= 0x08;
    CHECK(::pwrite(fd, &byte, 1, (off_t)off) == 1);
    ::close(fd);

    auto store = BlobStore::open(opt);
    CHECK_THROWS(store->load(victim));          // corrupted record rejected
    CHECK_EQ(*store->load(h[1]), payloads[1]);  // neighbour in same segment survives
    CHECK_EQ(*store->load(h[20]), payloads[20]); // record in another segment survives
}

// In-scope error point: a crash mid-pwrite that leaves a valid header but a
// truncated payload. Recovery must drop that torn record (bounds check) and keep
// all complete prior records; the torn record's handle then fails to load.
TEST("truncated payload at the tail is dropped, prior records survive") {
    TmpDir d("truncpayload");
    std::mt19937_64 rng(2);
    auto store = BlobStore::open({.dir = d.path});  // default large segment: all in seg 1
    std::vector<Index> h;
    std::vector<std::string> payloads;
    for (int i = 0; i < 20; ++i) {
        std::string p(500, char('A' + i));
        payloads.push_back(p);
        h.push_back(store->store(p));
    }
    store->sync();
    store.reset();  // close

    Index last = h.back();
    std::string path = seg_path(d.path, last.segment);
    // Chop the file in the middle of the last record's payload.
    ::truncate(path.c_str(), (off_t)(last.offset + 12 + last.length / 2));

    auto store2 = BlobStore::open({.dir = d.path});
    for (int i = 0; i < 19; ++i) CHECK_EQ(*store2->load(h[i]), payloads[i]);
    CHECK_THROWS(store2->load(last));  // torn record gone
    auto fresh = store2->store(std::string_view("post"));
    CHECK_EQ(*store2->load(fresh), std::string("post"));
}

// In-scope error point: a crash that left only a partial record header (< 12 bytes)
// at the tail. The scan's header-bounds guard must stop cleanly.
TEST("partial record header at the tail is dropped") {
    TmpDir d("partialhdr");
    auto store = BlobStore::open({.dir = d.path});
    std::vector<Index> h;
    for (int i = 0; i < 10; ++i) h.push_back(store->store(std::string_view("rec")));
    store->sync();
    store.reset();

    Index last = h.back();
    std::string path = seg_path(d.path, last.segment);
    ::truncate(path.c_str(), (off_t)(last.offset + 5));  // 5 bytes of a 12-byte header

    auto store2 = BlobStore::open({.dir = d.path});
    for (int i = 0; i < 9; ++i) CHECK_EQ(*store2->load(h[i]), std::string("rec"));
    CHECK_THROWS(store2->load(last));
    CHECK_EQ(file_size(path), last.offset);  // truncated exactly to last good record
}

// OsBuffered mode: after an explicit sync(), data must survive a clean reopen.
TEST("OsBuffered durability after explicit sync survives reopen") {
    TmpDir d("osbuffered");
    std::array<uint8_t, 16> saved{};
    {
        BlobStore::Options opt{.dir = d.path};
        opt.durability = Durability::OsBuffered;
        auto store = BlobStore::open(opt);
        saved = store->store(std::string_view("buffered-then-synced")).bytes();
        store->sync();
    }
    auto store = BlobStore::open({.dir = d.path});
    CHECK_EQ(*store->load(Index::from_bytes(saved)), std::string("buffered-then-synced"));
}

RUN_ALL_TESTS()
