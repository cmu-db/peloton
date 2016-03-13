/* Copyright (c) 2011-2012 Stanford University
 *
 * Copyright (c) 2011 Facebook
 *    startsWith() and endsWith() tests
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

#include "Core/StringUtil.h"

namespace LogCabin {
namespace Core {
namespace StringUtil {
namespace {

TEST(CoreStringUtilTest, flags) {
    enum {
        A = 1,
        B = 2,
        C = 4,
        D = 8,
    };
    std::initializer_list<std::pair<int, const char*>> abc = {
        {A, "A"},
        {B, "B"},
        {C, "C"},
    };

    EXPECT_EQ("0",
              flags(0, abc));
    EXPECT_EQ("A",
              flags(A, abc));
    EXPECT_EQ("A|B",
              flags(B|A, abc));
    EXPECT_EQ("A|B|0x8",
              flags(B|A|D, abc));
}

// Tests for format come from the RAMCloud project.
TEST(CoreStringUtilTest, formatBasic) {
    EXPECT_EQ("rofl3", format("rofl3"));
    EXPECT_EQ("rofl3", format("r%sl%d", "of", 3));
}

TEST(CoreStringUtilTest, formatLarge) {
    char x[3000];
    memset(x, 0xcc, sizeof(x));
    x[sizeof(x) - 1] = '\0';
    EXPECT_EQ(x, format("%s", x));
}

TEST(CoreStringUtilTest, isPrintableStr) {
    EXPECT_TRUE(isPrintable(""));
    EXPECT_TRUE(isPrintable("foo"));
    EXPECT_FALSE(isPrintable("\n"));
}

TEST(CoreStringUtilTest, isPrintableData) {
    EXPECT_FALSE(isPrintable("", 0));
    EXPECT_TRUE(isPrintable("", 1));
    EXPECT_TRUE(isPrintable("foo", 4));
    EXPECT_FALSE(isPrintable("foo", 3));
    EXPECT_FALSE(isPrintable("\n", 2));
}

TEST(CoreStringUtilTest, join) {
    EXPECT_EQ("",
              join(std::vector<std::string>{}, ","));
    EXPECT_EQ("a",
              join(std::vector<std::string>{"a"}, ","));
    EXPECT_EQ("abc;def;ghi",
              join(std::vector<std::string>{"abc", "def", "ghi"}, ";"));
    EXPECT_EQ(";abc\n;def;;",
              join(std::vector<std::string>{"", "abc\n", "def", "", ""}, ";"));
}

TEST(CoreStringUtilTest, split) {
    EXPECT_EQ((std::vector<std::string>{"abc", "def", "ghi"}),
              split("abc;def;ghi", ';'));
    EXPECT_EQ((std::vector<std::string>{"", "abc\n", "def", "", ""}),
              split(";abc\n;def;;;", ';'));
}

TEST(CoreStringUtilTest, startsWith) {
    EXPECT_TRUE(startsWith("foo", "foo"));
    EXPECT_TRUE(startsWith("foo", "fo"));
    EXPECT_TRUE(startsWith("foo", ""));
    EXPECT_TRUE(startsWith("", ""));
    EXPECT_FALSE(startsWith("f", "foo"));
}

TEST(CoreStringUtilTest, endsWith) {
    EXPECT_TRUE(endsWith("foo", "foo"));
    EXPECT_TRUE(endsWith("foo", "oo"));
    EXPECT_TRUE(endsWith("foo", ""));
    EXPECT_TRUE(endsWith("", ""));
    EXPECT_FALSE(endsWith("o", "foo"));
}

TEST(CoreStringUtilTest, toString) {
    EXPECT_EQ("3", toString(3));
}

TEST(CoreStringUtilTest, trim) {
    EXPECT_EQ("abc", trim("abc"));
    EXPECT_EQ("abc", trim(" abc "));
    EXPECT_EQ("abc", trim("\tabc\n"));
    EXPECT_EQ("", trim("  "));
}

} // namespace LogCabin::Core::StringUtil::<anonymous>
} // namespace LogCabin::Core::StringUtil
} // namespace LogCabin::Core
} // namespace LogCabin
