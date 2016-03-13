/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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
#include <unistd.h>

#include "Core/Random.h"

namespace LogCabin {
namespace Core {
namespace Random {
namespace {

TEST(CoreRandomTest, fork) {
    // failures counts the number of attempts that the parent and child chose
    // the same random value. This is expected attempt/256 times.
    uint64_t failures = 0;
    for (uint64_t attempt = 0; attempt < 16; ++attempt) {
        errno = 0;
        pid_t pid = fork();
        ASSERT_NE(pid, -1) << strerror(errno); // error
        if (pid == 0) { // child
            _exit(random8());
        } else { // parent
            uint8_t parent = random8();
            int status = 0;
            int r = waitpid(pid, &status, 0);
            ASSERT_EQ(pid, r);
            ASSERT_TRUE(WIFEXITED(status));
            uint8_t child = uint8_t(WEXITSTATUS(status));
            if (parent == child)
                ++failures;
        }
    }
    EXPECT_GT(2U, failures) << failures;
}

TEST(CoreRandomTest, bitCoverage8) {
    uint8_t r = 0;
    for (uint32_t i = 0; i < 20; ++i)
       r = uint8_t(r | random8());
    EXPECT_EQ(0xFF, r);
}

TEST(CoreRandomTest, bitCoverage16) {
    uint16_t r = 0;
    for (uint32_t i = 0; i < 20; ++i)
       r = uint16_t(r | random16());
    EXPECT_EQ(0xFFFF, r);
}

TEST(CoreRandomTest, bitCoverage32) {
    uint32_t r = 0;
    for (uint32_t i = 0; i < 20; ++i)
       r = uint32_t(r | random32());
    EXPECT_EQ(~0U, r);
}

TEST(CoreRandomTest, bitCoverage64) {
    uint64_t r = 0;
    for (uint32_t i = 0; i < 20; ++i)
       r = uint64_t(r | random64());
    EXPECT_EQ(~0UL, r);
}

TEST(CoreRandomTest, randomRangeDouble) {
    EXPECT_LT(0.0, randomRangeDouble(0.0, 1.0));
    EXPECT_GT(1.0, randomRangeDouble(0.0, 1.0));
    EXPECT_LT(2.0, randomRangeDouble(2.0, 3.0));
    EXPECT_GT(3.0, randomRangeDouble(2.0, 3.0));
    EXPECT_LT(1.5, randomRangeDouble(1.5, 1.6));
    EXPECT_GT(1.6, randomRangeDouble(1.5, 1.6));
    EXPECT_LT(-0.5, randomRangeDouble(-0.5, 0.5));
    EXPECT_GT(0.5, randomRangeDouble(-0.5, 0.5));
    EXPECT_EQ(10.0, randomRangeDouble(10.0, 10.0));
    EXPECT_NE(randomRangeDouble(0.0, 1.0),
              randomRangeDouble(0.0, 1.0));
}

// make sure randomRangeDouble just works if its arguments are reversed
TEST(CoreRandomTest, randomRangeDouble_reversed) {
    EXPECT_LT(0.0, randomRangeDouble(1.0, 0.0));
    EXPECT_GT(1.0, randomRangeDouble(1.0, 0.0));
    EXPECT_LT(2.0, randomRangeDouble(3.0, 2.0));
    EXPECT_GT(3.0, randomRangeDouble(3.0, 2.0));
    EXPECT_LT(1.5, randomRangeDouble(1.6, 1.5));
    EXPECT_GT(1.6, randomRangeDouble(1.6, 1.5));
    EXPECT_LT(-0.5, randomRangeDouble(0.5, -0.5));
    EXPECT_GT(0.5, randomRangeDouble(0.5, -0.5));
    EXPECT_EQ(10.0, randomRangeDouble(10.0, 10.0));
    EXPECT_NE(randomRangeDouble(1.0, 0.0),
              randomRangeDouble(1.0, 0.0));
}

TEST(CoreRandomTest, randomRangeInt) {
    uint64_t ones = 0;
    uint64_t twos = 0;
    for (uint32_t i = 0; i < 20; i++) {
        uint64_t r = randomRange(1, 2);
        EXPECT_LE(1U, r);
        EXPECT_GE(2U, r);
        if (r == 1)
            ++ones;
        else if (r == 2)
            ++twos;
    }
    EXPECT_LT(0U, ones);
    EXPECT_LT(0U, twos);

    EXPECT_LE(0U, randomRange(0, 10));
    EXPECT_GE(10U, randomRange(0, 10));
    EXPECT_LE(20U, randomRange(20, 30));
    EXPECT_GE(30U, randomRange(20, 30));
    EXPECT_LE(15U, randomRange(15, 16));
    EXPECT_GE(16U, randomRange(15, 16));
    EXPECT_EQ(10U, randomRange(10, 10));
    EXPECT_NE(randomRange(0, 10000),
              randomRange(0, 10000));
}

// make sure randomRange just works if its arguments are reversed
TEST(CoreRandomTest, randomRange_reversed) {

    EXPECT_LE(0U, randomRange(10, 0));
    EXPECT_GE(10U, randomRange(10, 0));
    EXPECT_LE(20U, randomRange(30, 20));
    EXPECT_GE(30U, randomRange(30, 20));
    EXPECT_LE(15U, randomRange(16, 15));
    EXPECT_GE(16U, randomRange(16, 15));
    EXPECT_EQ(10U, randomRange(10, 10));
    EXPECT_NE(randomRange(10000, 0),
              randomRange(10000, 0));
}

} // namespace LogCabin::Core::Random::<anonymous>
} // namespace LogCabin::Core::Random
} // namespace LogCabin::Core
} // namespace LogCabin
