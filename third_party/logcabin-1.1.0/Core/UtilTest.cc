/* Copyright (c) 2011-2012 Stanford University
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

#include "Core/Util.h"

namespace LogCabin {
namespace Core {
namespace Util {
namespace {

TEST(CoreUtilTest, downCast) {
#if !NDEBUG
    EXPECT_DEATH(downCast<uint8_t>(256), "");
    EXPECT_DEATH(downCast<int8_t>(192), "");
    EXPECT_DEATH(downCast<uint8_t>(-10), "");
#endif
    uint8_t x = downCast<uint8_t>(55UL);
    EXPECT_EQ(55, x);
}

TEST(CoreUtilTest, sizeof32) {
    EXPECT_EQ(8U, sizeof32(uint64_t));
}

void setToWorld(std::string& s)
{
    s = "world";
}

TEST(CoreUtilTest, Finally) {
    std::string s = "hello";
    {
        Finally _(std::bind(setToWorld, std::ref(s)));
        EXPECT_EQ("hello", s);
    }
    EXPECT_EQ("world", s);
}

TEST(CoreUtilTest, isPowerOfTwo) {
    EXPECT_FALSE(isPowerOfTwo(0));
    EXPECT_TRUE(isPowerOfTwo(1));
    EXPECT_TRUE(isPowerOfTwo(2));
    EXPECT_FALSE(isPowerOfTwo(3));
    EXPECT_TRUE(isPowerOfTwo(4));
    EXPECT_FALSE(isPowerOfTwo(5));
    EXPECT_FALSE(isPowerOfTwo(6));
    EXPECT_FALSE(isPowerOfTwo(7));
    EXPECT_TRUE(isPowerOfTwo(8));
    EXPECT_FALSE(isPowerOfTwo(9));
    EXPECT_FALSE(isPowerOfTwo(10));
}

TEST(CoreUtilTest, memcpy) {
    char buf[16];
    memset(buf, 0, sizeof(buf));
    EXPECT_EQ(buf, memcpy(buf, {{"hello ", 6}, {"world!", 7}}));
    EXPECT_STREQ("hello world!", buf);
}

TEST(CoreUtilTest, ThreadInterruptedException) {
    EXPECT_THROW(throw ThreadInterruptedException(),
                 ThreadInterruptedException);
    EXPECT_STREQ("Thread was interrupted",
                 ThreadInterruptedException().what());
}

} // namespace LogCabin::Core::Util::<anonymous>
} // namespace LogCabin::Core::Util
} // namespace LogCabin::Core
} // namespace LogCabin
