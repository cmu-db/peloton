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

#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "build/Core/ProtoBufTest.pb.h"

namespace LogCabin {
namespace Core {
namespace ProtoBuf {

using LogCabin::ProtoBuf::TestMessage;
using LogCabin::ProtoBuf::MissingOld;
using LogCabin::ProtoBuf::MissingNew;

TEST(CoreProtoBufTest, equality) {
    TestMessage a;
    TestMessage b;
    EXPECT_EQ(a, a);
    EXPECT_EQ(a, b);
    b.set_field_a(3);
    EXPECT_NE(a, b);
    EXPECT_NE(b, a);
}

TEST(CoreProtoBufTest, equalityStr) {
    // The protobuf ERRORs during this test are normal.
    TestMessage m;
    EXPECT_EQ(m, "");
    EXPECT_EQ("", m);
    m.set_field_a(3);
    EXPECT_NE(m, "");
    EXPECT_NE("", m);
    EXPECT_EQ(m, "field_a: 3");
    EXPECT_EQ("field_a: 3", m);
}

TEST(CoreProtoBufTest, fromString) {
    TestMessage m;
    m = fromString<TestMessage>("field_a: 3, field_b: 5");
    EXPECT_EQ("field_a: 3 field_b: 5", m.ShortDebugString());

    // missing fields
    m = fromString<TestMessage>("");
    EXPECT_EQ("", m.ShortDebugString());
}

TEST(CoreProtoBufTest, dumpString) {
    TestMessage m;
    m.set_field_a(3);
    m.set_field_b(5);
    m.add_field_c(12);
    m.add_field_c(19);
    m.set_field_d("apostr'phe bin\01\02ry");
    // Don't really care about the exact output, but it should be printable.
    EXPECT_EQ("field_a: 3\n"
              "field_b: 5\n"
              "field_c: [12, 19]\n"
              "field_d: \"apostr\\'phe bin\\001\\002ry\"\n",
              dumpString(m, false));
    EXPECT_EQ("              \"field_a: 3\"\n"
              "              \"field_b: 5\"\n"
              "              \"field_c: [12, 19]\"\n"
              "              \"field_d: 'apostr\\'phe bin\\001\\002ry'\"\n",
              dumpString(m, true));
}

TEST(CoreProtoBufTest, copy) {
    TestMessage m;
    m = fromString<TestMessage>("field_a: 3, field_b: 5");
    EXPECT_EQ(*copy(m), m);
}

TEST(CoreProtoBufTest, parse) {
    Core::Buffer rpc;
    TestMessage m;
    Core::Debug::setLogPolicy({{"", "ERROR"}});
    EXPECT_FALSE(ProtoBuf::parse(rpc, m));
    Core::Debug::setLogPolicy({});
    m.set_field_a(3);
    m.set_field_b(5);
    ProtoBuf::serialize(m, rpc, 8);
    *static_cast<uint64_t*>(rpc.getData()) = 0xdeadbeefdeadbeef;
    m.Clear();
    EXPECT_TRUE(ProtoBuf::parse(rpc, m, 8));
    EXPECT_EQ("field_a: 3 field_b: 5", m.ShortDebugString());
}

TEST(CoreProtoBufTest, serialize) {
    Core::Buffer rpc;
    TestMessage m;
    EXPECT_DEATH(ProtoBuf::serialize(m, rpc, 3),
                 "Missing fields in protocol buffer.*: field_a, field_b");
    m.set_field_a(3);
    m.set_field_b(5);
    ProtoBuf::serialize(m, rpc, 8);
    *static_cast<uint64_t*>(rpc.getData()) = 0xdeadbeefdeadbeef;
    m.Clear();
    EXPECT_TRUE(ProtoBuf::parse(rpc, m, 8));
    EXPECT_EQ("field_a: 3 field_b: 5", m.ShortDebugString());
}

// See https://github.com/logcabin/logcabin/issues/89:
//
// If we have a required enum field, an unknown value will cause the field to
// be missing when it's parsed.
//
// If we have an optional enum field, an unknown value will be equal to the
// first value listed in the enum, yet will serialize to the unknown value.
TEST(CoreProtoBufTest, missingEnum) {
    MissingNew mnew;
    mnew.set_which(MissingNew::FOUR);

    Core::Buffer buf;
    ProtoBuf::serialize(mnew, buf);
    MissingOld mold;
    EXPECT_TRUE(ProtoBuf::parse(buf, mold));
    EXPECT_TRUE(mnew.has_which());
    EXPECT_EQ(MissingOld::UNKNOWN, mold.which());
    EXPECT_EQ(90, mold.which());

    buf.reset();
    mnew.Clear();
    ProtoBuf::serialize(mold, buf);
    EXPECT_TRUE(ProtoBuf::parse(buf, mnew));
    EXPECT_TRUE(mnew.has_which());
    EXPECT_EQ(MissingNew::FOUR, mnew.which());
}

// missing optional primitives seem to work as expected
TEST(CoreProtoBufTest, missingPrimitive) {
    MissingNew mnew;
    mnew.set_primitive(3);

    Core::Buffer buf;
    ProtoBuf::serialize(mnew, buf);
    MissingOld mold;
    EXPECT_TRUE(ProtoBuf::parse(buf, mold));

    buf.reset();
    mnew.Clear();
    ProtoBuf::serialize(mold, buf);
    EXPECT_TRUE(ProtoBuf::parse(buf, mnew));
    EXPECT_TRUE(mnew.has_primitive());
    EXPECT_EQ(3U, mnew.primitive());
}

// missing optional nested messages seem to work as expected
TEST(CoreProtoBufTest, missingMessage) {
    MissingNew mnew;
    mnew.mutable_msg()->set_field_a(30);
    mnew.mutable_msg()->set_field_b(40);

    Core::Buffer buf;
    ProtoBuf::serialize(mnew, buf);
    MissingOld mold;
    EXPECT_TRUE(ProtoBuf::parse(buf, mold));

    buf.reset();
    mnew.Clear();
    ProtoBuf::serialize(mold, buf);
    EXPECT_TRUE(ProtoBuf::parse(buf, mnew));
    EXPECT_TRUE(mnew.has_msg());
    EXPECT_EQ(30U, mnew.msg().field_a());
    EXPECT_EQ(40U, mnew.msg().field_b());
}


} // namespace LogCabin::Core::ProtoBuf
} // namespace LogCabin::Core
} // namespace LogCabin
