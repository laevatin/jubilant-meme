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

// Frame one record at dst: [magic|len|crc][payload|footer=len]. Returns total bytes.
inline uint64_t frame_record(char* dst, const void* data, size_t len) {
    put_u32(dst + 0, kMagic);
    put_u32(dst + 4, (uint32_t)len);
    uint32_t crc = crc32c(dst, 8);                    // cover magic + len
    crc = crc32c(data, len, crc);                     // cover payload
    put_u32(dst + 8, crc);
    if (len) std::memcpy(dst + kHeaderSize, data, len);
    put_u32(dst + kHeaderSize + len, (uint32_t)len);  // footer (torn-write delimiter)
    return kHeaderSize + len + kFooterSize;
}

}  // namespace fmt
}  // namespace bs
