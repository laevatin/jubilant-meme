// Spec: CRC-32C must match known vectors and detect single-bit corruption.
#include "crc32c.h"
#include "test_framework.h"

#include <string>

using bs::crc32c;

TEST("crc32c known vectors") {
    // Standard CRC-32C (Castagnoli) check value for "123456789".
    CHECK_EQ(crc32c("123456789", 9), 0xE3069283u);
    CHECK_EQ(crc32c("", 0), 0x00000000u);
}

TEST("crc32c is incremental") {
    std::string s = "the quick brown fox";
    uint32_t whole = crc32c(s.data(), s.size());
    uint32_t part = crc32c(s.data(), 10);
    part = crc32c(s.data() + 10, s.size() - 10, part);
    CHECK_EQ(whole, part);
}

TEST("crc32c detects a flipped bit") {
    std::string a(64, 'x');
    std::string b = a;
    b[37] ^= 0x08;
    CHECK(crc32c(a.data(), a.size()) != crc32c(b.data(), b.size()));
}

RUN_ALL_TESTS()
