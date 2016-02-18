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

#include "Core/Buffer.h"

namespace LogCabin {
namespace Core {
namespace {

uint32_t deleterCount;

void
deleterCounter(void* data)
{
    ++deleterCount;
}

class CoreBufferTest : public ::testing::Test {
  public:
    CoreBufferTest()
    {
        deleterCount = 0;
        strncpy(buf, "foo", sizeof(buf));
    }
    char buf[4];
};


TEST_F(CoreBufferTest, constructor_default) {
    Buffer buffer;
    EXPECT_TRUE(NULL == buffer.getData());
    EXPECT_EQ(0U, buffer.getLength());
}

TEST_F(CoreBufferTest, constructor_withData) {
    {
        Buffer buffer(buf, sizeof(buf), deleterCounter);
        EXPECT_EQ(buf, buffer.getData());
        EXPECT_EQ(sizeof(buf), buffer.getLength());
    }
    EXPECT_EQ(1U, deleterCount);
}

TEST_F(CoreBufferTest, constructor_move) {
    {
        Buffer buffer1(buf, sizeof(buf), deleterCounter);
        Buffer buffer2(std::move(buffer1));
        EXPECT_TRUE(NULL == buffer1.getData());
        EXPECT_EQ(0U, buffer1.getLength());
        EXPECT_EQ(buf, buffer2.getData());
        EXPECT_EQ(sizeof(buf), buffer2.getLength());
    }
    EXPECT_EQ(1U, deleterCount);
}

TEST_F(CoreBufferTest, destructor) {
    {
        Buffer buffer1(buf, sizeof(buf), deleterCounter);
        Buffer buffer2(buf, sizeof(buf), NULL);
    }
    EXPECT_EQ(1U, deleterCount);
}

TEST_F(CoreBufferTest, assignment_move) {
    {
        Buffer buffer1(buf, sizeof(buf), deleterCounter);
        Buffer buffer2(buf + 1, sizeof(buf) - 1, deleterCounter);
        buffer2 = std::move(buffer1);
        EXPECT_TRUE(NULL == buffer1.getData());
        EXPECT_EQ(0U, buffer1.getLength());
        EXPECT_EQ(buf, buffer2.getData());
        EXPECT_EQ(sizeof(buf), buffer2.getLength());
    }
    EXPECT_EQ(2U, deleterCount);
}

TEST_F(CoreBufferTest, getters_nonconst) {
    Buffer buffer(buf, sizeof(buf), deleterCounter);
    EXPECT_EQ(buf, buffer.getData());
    EXPECT_EQ(sizeof(buf), buffer.getLength());
}

TEST_F(CoreBufferTest, getters_const) {
    const Buffer buffer(buf, sizeof(buf), deleterCounter);
    EXPECT_EQ(buf, buffer.getData());
    EXPECT_EQ(sizeof(buf), buffer.getLength());
}

TEST_F(CoreBufferTest, setData) {
    {
        Buffer buffer;
        buffer.setData(buf, sizeof(buf), deleterCounter);
        EXPECT_EQ(buf, buffer.getData());
        EXPECT_EQ(sizeof(buf), buffer.getLength());
        buffer.setData(buf + 1, sizeof(buf) - 1, deleterCounter);
        EXPECT_EQ(buf + 1, buffer.getData());
        EXPECT_EQ(sizeof(buf) - 1, buffer.getLength());
    }
    EXPECT_EQ(2U, deleterCount);
}

TEST_F(CoreBufferTest, reset) {
    {
        Buffer buffer;
        buffer.reset();
        buffer.setData(buf, sizeof(buf), deleterCounter);
        buffer.reset();
        EXPECT_EQ(1U, deleterCount);
        EXPECT_TRUE(NULL == buffer.getData());
        EXPECT_EQ(0U, buffer.getLength());
    }
    EXPECT_EQ(1U, deleterCount);
}

} // namespace LogCabin::Core::<anonymous>
} // namespace LogCabin::Core
} // namespace LogCabin
