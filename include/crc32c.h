// crc32c.h — CRC-32C (Castagnoli, poly 0x1EDC6F41 / reversed 0x82F63B78).
// Software table implementation; dependency-free. Used to detect torn writes.
#pragma once

#include <cstddef>
#include <cstdint>

namespace bs {

inline const uint32_t* crc32c_table() {
    static uint32_t t[256];
    static const bool init = [] {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k)
                c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return true;
    }();
    (void)init;
    return t;
}

// Incremental: pass the previous return value as `crc` to continue a stream.
inline uint32_t crc32c(const void* data, size_t n, uint32_t crc = 0) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    const uint32_t* t = crc32c_table();
    crc = ~crc;
    for (size_t i = 0; i < n; ++i)
        crc = t[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

}  // namespace bs
