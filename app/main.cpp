// main.cpp — small driver demonstrating the decoupled RecordWriter / RecordReader.
//
// Usage:
//   main <dir> write <N>      append N demo records, then close
//   main <dir> read           load every record back via a forward scan
//   main <dir>                 write 5 then read them back (default demo)
#include "record_reader.h"
#include "record_writer.h"

#include <cstdio>
#include <cstdlib>
#include <string>

using namespace bs;

static int do_write(const std::string& dir, int n) {
    Options opt;
    opt.dir = dir;
    auto w = RecordWriter::open(opt);
    for (int i = 0; i < n; ++i) {
        Index idx = w->append("record-" + std::to_string(i));
        std::printf("wrote #%d -> seg=%u len=%u off=%llu\n", i, idx.segment, idx.length,
                    (unsigned long long)idx.offset);
    }
    w->close();
    auto st = w->stats();
    std::printf("appends=%llu bytes=%llu fsyncs=%llu\n",
                (unsigned long long)st.appends, (unsigned long long)st.bytes_written,
                (unsigned long long)st.fsyncs);
    return 0;
}

static int do_read(const std::string& dir) {
    Options opt;
    opt.dir = dir;
    auto r = RecordReader::open(opt);
    uint64_t n = 0;
    for (const auto& rec : r->scan()) {
        std::printf("read off=%llu len=%u: %s\n", (unsigned long long)rec.index.offset,
                    rec.index.length, rec.value.c_str());
        ++n;
    }
    std::printf("scanned %llu records\n", (unsigned long long)n);
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <dir> [write N | read]\n", argv[0]);
        return 2;
    }
    std::string dir = argv[1];
    std::string cmd = argc >= 3 ? argv[2] : "";

    if (cmd == "write") return do_write(dir, argc >= 4 ? std::atoi(argv[3]) : 5);
    if (cmd == "read") return do_read(dir);

    // Default demo: write then read.
    do_write(dir, 5);
    return do_read(dir);
}
