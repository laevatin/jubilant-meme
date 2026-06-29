// record_format.h — INTERNAL on-disk format helpers shared by the writer and the
// reader implementations. Not part of the public API; lives under src/.
//
// Record framing: [magic(4) | len(4) | crc32c(4)] [payload(len)] [footer=len(4)]
// little-endian. The CRC covers the header (magic+len) AND the payload, so a flip
// anywhere in the framing is caught at load(). The footer repeats len as a
// torn-write delimiter so recovery can tell "ended where the header claimed".
#pragma once

#include "crc32c.h"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace bs {
namespace fmt {

constexpr uint32_t kMagic        = 0x424C4F42;   // 'BLOB'
constexpr uint64_t kHeaderSize   = 12;           // magic(4) + len(4) + crc(4)
constexpr uint64_t kFooterSize   = 4;            // len(4)
constexpr uint32_t kSegmentId    = 1;            // the one and only segment
constexpr uint64_t kMaxSegmentSize = 0xFFFFFFFFull;  // offsets must fit a uint32

inline void put_u32(char* p, uint32_t v) {
    p[0] = char(v & 0xFF); p[1] = char((v >> 8) & 0xFF);
    p[2] = char((v >> 16) & 0xFF); p[3] = char((v >> 24) & 0xFF);
}
inline uint32_t get_u32(const char* p) {
    return uint32_t(uint8_t(p[0])) | (uint32_t(uint8_t(p[1])) << 8) |
           (uint32_t(uint8_t(p[2])) << 16) | (uint32_t(uint8_t(p[3])) << 24);
}

inline void pwrite_all(int fd, const char* buf, size_t n, uint64_t off) {
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
inline size_t pread_all(int fd, char* buf, size_t n, uint64_t off) {
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

inline std::string segment_path(const std::string& dir) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06u.seg", kSegmentId);
    return dir + "/" + name;
}

// The record CRC: covers the first 8 header bytes (magic+len) then the payload.
// One definition so the write and read paths can never disagree on what's covered.
inline uint32_t crc_record(const char* hdr8, const void* payload, size_t len) {
    uint32_t crc = crc32c(hdr8, 8);
    return crc32c(payload, len, crc);
}

// Frame one record at dst: [magic|len|crc][payload|footer=len]. Returns total bytes.
inline uint64_t frame_record(char* dst, const void* data, size_t len) {
    put_u32(dst + 0, kMagic);
    put_u32(dst + 4, (uint32_t)len);
    put_u32(dst + 8, crc_record(dst, data, len));
    if (len) std::memcpy(dst + kHeaderSize, data, len);
    put_u32(dst + kHeaderSize + len, (uint32_t)len);  // footer (torn-write delimiter)
    return kHeaderSize + len + kFooterSize;
}

// ---- record parsing (shared by recovery, scan, and load) -------------------

// A parsed record header. `ok == false` means a framing break at this offset: a
// short read, bad magic, or a record that would extend past `limit` (a torn tail
// or EOF). When ok, `reclen` is the record's total on-disk size.
struct FrameHeader {
    bool     ok     = false;
    uint32_t len    = 0;             // payload length
    uint32_t crc    = 0;             // expected CRC (covers header[0..8) + payload)
    uint64_t reclen = 0;             // kHeaderSize + len + kFooterSize
    char     raw[kHeaderSize] = {};  // the raw header bytes (for the CRC)
};

// Read and framing-check the 12-byte header at `off`, never reading past `limit`.
// This is the read+check step common to recovery, scan, and load.
inline FrameHeader read_header(int fd, uint64_t off, uint64_t limit) {
    FrameHeader h;
    if (off + kHeaderSize > limit) return h;                          // runs past end
    if (pread_all(fd, h.raw, kHeaderSize, off) != kHeaderSize) return h;  // short read
    if (get_u32(h.raw) != kMagic) return h;                          // framing gone
    h.len = get_u32(h.raw + 4);
    h.reclen = kHeaderSize + (uint64_t)h.len + kFooterSize;
    if (off + h.reclen > limit) return h;                            // record runs past end
    h.crc = get_u32(h.raw + 8);
    h.ok = true;
    return h;
}

// Result of checking a record's body (`body` = len payload bytes followed by the
// 4-byte footer) against its header.
enum class BodyCheck { Ok, BadFooter, BadCrc };

// Verify the footer (a second copy of len) and the CRC. Call only on a header that
// read_header returned ok for.
inline BodyCheck check_body(const char* hdr8, uint32_t len, uint32_t crc_want, const char* body) {
    if (get_u32(body + len) != len) return BodyCheck::BadFooter;
    if (crc_record(hdr8, body, len) != crc_want) return BodyCheck::BadCrc;
    return BodyCheck::Ok;
}

}  // namespace fmt
}  // namespace bs
