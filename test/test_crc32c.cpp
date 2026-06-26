// Spec: CRC-32C must match known vectors and detect single-bit corruption.
#include "crc32c.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>

using bs::crc32c;

TEST(Crc32c, KnownVectors) {
    // Standard CRC-32C (Castagnoli) check value for "123456789".
    EXPECT_EQ(crc32c("123456789", 9), 0xE3069283u);
    EXPECT_EQ(crc32c("", 0), 0x00000000u);
}

TEST(Crc32c, IsIncremental) {
    std::string s = "the quick brown fox";
    uint32_t whole = crc32c(s.data(), s.size());
    uint32_t part = crc32c(s.data(), 10);
    part = crc32c(s.data() + 10, s.size() - 10, part);
    EXPECT_EQ(whole, part);
}

TEST(Crc32c, DetectsAFlippedBit) {
    std::string a(64, 'x');
    std::string b = a;
    b[37] ^= 0x08;
    EXPECT_NE(crc32c(a.data(), a.size()), crc32c(b.data(), b.size()));
}

// The hardware (SSE4.2) and software paths must agree bit-for-bit across all length
// classes (including the 8-byte-block boundary and odd byte tails), one-shot and
// incrementally split — otherwise a record written on one path can't be verified on
// the other.
TEST(Crc32c, HardwareMatchesSoftware) {
#ifdef BS_CRC32C_X86
    if (!bs::crc32c_have_hw()) GTEST_SKIP() << "no SSE4.2 on this CPU";
    std::mt19937_64 rng(0x5EED);
    for (size_t n : {0u, 1u, 2u, 3u, 7u, 8u, 9u, 15u, 16u, 17u, 31u, 64u, 1000u, 65537u}) {
        std::string s(n, '\0');
        for (auto& c : s) c = char(rng() & 0xFF);
        EXPECT_EQ(bs::crc32c_hw(s.data(), n, 0), bs::crc32c_sw(s.data(), n, 0)) << "n=" << n;
        // Incremental split must also agree across paths.
        size_t mid = n / 2;
        uint32_t hw = bs::crc32c_hw(s.data() + mid, n - mid, bs::crc32c_hw(s.data(), mid, 0));
        uint32_t sw = bs::crc32c_sw(s.data() + mid, n - mid, bs::crc32c_sw(s.data(), mid, 0));
        EXPECT_EQ(hw, sw) << "n=" << n << " (incremental)";
        EXPECT_EQ(hw, bs::crc32c_sw(s.data(), n, 0)) << "n=" << n << " (incremental == one-shot)";
    }
#else
    GTEST_SKIP() << "not an x86 target";
#endif
}
