/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <gtest/gtest.h>

#include "Core/Checksum.h"

namespace LogCabin {
namespace Core {
namespace Checksum {
namespace {

class CoreChecksumTest : public ::testing::Test {
  public:
    CoreChecksumTest()
    {
        memset(buf, 'C', sizeof(buf));
    }
    char buf[300];
};

TEST_F(CoreChecksumTest, listAlgorithms) {
    EXPECT_EQ((std::vector<std::string> {
                   "Adler32",
                   "CRC32",
                   "MD5",
                   "RIPEMD-128",
                   "RIPEMD-160",
                   "RIPEMD-256",
                   "RIPEMD-320",
                   "SHA-1",
                   "SHA-224",
                   "SHA-256",
                   "SHA-384",
                   "SHA-512",
                   "Tiger",
                   "Whirlpool",
              }),
              listAlgorithms());
}

TEST_F(CoreChecksumTest, calculate) {
    uint32_t sha1OutputLen = 6  /* 'SHA-1:' */ +
                             40 /* hex digest */ +
                             1  /* null terminator */;
    char output[MAX_LENGTH];
    EXPECT_EQ(sha1OutputLen, calculate("SHA-1", "test", 0, output));
    EXPECT_STREQ("SHA-1:da39a3ee5e6b4b0d3255bfef95601890afd80709",
                 output);
    EXPECT_EQ(sha1OutputLen, calculate("SHA-1", "test", 5, output));
    EXPECT_STREQ("SHA-1:961fa64958818f767707072755d7018dcd278e94",
                 output);
    EXPECT_EQ(sha1OutputLen, calculate("SHA-1",
                                       {{"te", 2},
                                        {"", 0},
                                        {"st", 3}}, output));
    EXPECT_STREQ("SHA-1:961fa64958818f767707072755d7018dcd278e94",
                 output);
    EXPECT_DEATH(calculate("nonsense", "test", 5, output),
                 "not available");
}

TEST_F(CoreChecksumTest, lengthReasonable) {
    strcpy(buf, "mock:1234"); // NOLINT
    EXPECT_EQ(10U, length(buf, sizeof(buf)));
}

TEST_F(CoreChecksumTest, length0Max) {
    strcpy(buf, ""); // NOLINT
    EXPECT_EQ(0U, length(buf, 0));
}

TEST_F(CoreChecksumTest, lengthTruncatingMax) {
    strcpy(buf, "mock:1234"); // NOLINT
    EXPECT_EQ(0U, length(buf, 9));
}

TEST_F(CoreChecksumTest, lengthTooLong) {
    EXPECT_EQ(0U, length(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        ":123",
        64 * 5 + 5));
}

TEST_F(CoreChecksumTest, verify) {
    EXPECT_EQ("",
              verify("SHA-1:961fa64958818f767707072755d7018dcd278e94",
                     "test", 5));
    EXPECT_EQ("",
              verify("SHA-1:961fa64958818f767707072755d7018dcd278e94",
                     {{"te", 2},
                      {"", 0},
                      {"st", 3}}));
    EXPECT_EQ("The given checksum value is corrupt and not printable.",
              verify("\n", "test", 5));
    EXPECT_EQ("Missing colon in checksum: SHA-1",
              verify("SHA-1", "test", 5));
    EXPECT_EQ("Checksum doesn't match: expected SHA-1:358 but calculated "
              "SHA-1:961fa64958818f767707072755d7018dcd278e94",
              verify("SHA-1:358", "test", 5));
    EXPECT_EQ("No such checksum algorithm: nonsense",
              verify("nonsense:358", "test", 5));
}

} // namespace LogCabin::Core::Checksum::<anonymous>
} // namespace LogCabin::Core::Checksum
} // namespace LogCabin::Core
} // namespace LogCabin
