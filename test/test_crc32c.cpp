// Spec: CRC-32C must match known vectors and detect single-bit corruption.
#include "crc32c.h"

#include <gtest/gtest.h>

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
