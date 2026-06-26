// crc32c.h — CRC-32C (Castagnoli, poly 0x1EDC6F41 / reversed 0x82F63B78).
//
// Hardware path: the x86 SSE4.2 `crc32` instruction computes CRC-32C directly, so
// `crc32c()` uses it when the CPU advertises sse4.2 (detected once at runtime) and
// falls back to a dependency-free software table otherwise. Both paths use the same
// init/final inversion (0xFFFFFFFF) and the same reflected polynomial, so they
// produce identical values — incremental calls may cross paths safely. Used to
// detect torn writes.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(__i386__)
#  include <immintrin.h>
#  define BS_CRC32C_X86 1
#endif

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

// Software fallback: table-driven, portable. Incremental via the `crc` seed.
inline uint32_t crc32c_sw(const void* data, size_t n, uint32_t crc) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    const uint32_t* t = crc32c_table();
    crc = ~crc;
    for (size_t i = 0; i < n; ++i)
        crc = t[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

#ifdef BS_CRC32C_X86
// Hardware path: SSE4.2 `crc32` over 8 bytes at a time, then the byte tail. The
// instruction does the raw reflected update, so we keep the standard init/final
// inversion around it to match crc32c_sw exactly. target(...) lets this one
// function use sse4.2 without forcing it on the whole translation unit.
__attribute__((target("sse4.2")))
inline uint32_t crc32c_hw(const void* data, size_t n, uint32_t crc) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t c = ~crc;
    while (n >= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);                       // unaligned-safe
        c = static_cast<uint32_t>(_mm_crc32_u64(c, v));
        p += 8;
        n -= 8;
    }
    while (n) { c = _mm_crc32_u8(c, *p); ++p; --n; }
    return ~c;
}

inline bool crc32c_have_hw() {
    static const bool ok = __builtin_cpu_supports("sse4.2");
    return ok;
}
#endif  // BS_CRC32C_X86

// Incremental: pass the previous return value as `crc` to continue a stream.
inline uint32_t crc32c(const void* data, size_t n, uint32_t crc = 0) {
#ifdef BS_CRC32C_X86
    if (crc32c_have_hw()) return crc32c_hw(data, n, crc);
#endif
    return crc32c_sw(data, n, crc);
}

}  // namespace bs
