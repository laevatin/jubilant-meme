// Spec: crash recovery. A torn write at the tail of the active segment must be
// truncated on reopen; all fully-written prior records must remain loadable, and
// the store must accept new writes afterward.
#include "blob_store.h"
#include "test_framework.h"

#include <cstdio>
#include <fcntl.h>
#include <string>
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

RUN_ALL_TESTS()
