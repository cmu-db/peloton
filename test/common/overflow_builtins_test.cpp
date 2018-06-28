//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// overflow_builtins_test.cpp
//
// Identification: test/common/overflow_builtins_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/overflow_builtins.h"

#include "common/harness.h"

#include <limits>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Testing the fallback functions for overflow aware operations
//===--------------------------------------------------------------------===//

class OverflowBuiltinsTest : public PelotonTest {
 public:
  using unsigned_t = unsigned int;
  using signed_t = int;
};

TEST_F(OverflowBuiltinsTest, UnsignedAddTest) {
  unsigned_t max = std::numeric_limits<unsigned_t>::max();
  unsigned_t min = std::numeric_limits<unsigned_t>::min();

  unsigned_t c;
  bool overflow;

  overflow = builtin_add_overflow<unsigned_t>(0, 3, &c);
  EXPECT_EQ(c, 3);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<unsigned_t>(0, 0, &c);
  EXPECT_EQ(c, 0);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<unsigned_t>(max - 12, 3, &c);
  EXPECT_EQ(c, max - 9);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<unsigned_t>(max - 12, 12, &c);
  EXPECT_EQ(c, max);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<unsigned_t>(max - 12, 13, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, true);

  overflow = builtin_add_overflow<unsigned_t>(max - 12, 21, &c);
  EXPECT_EQ(c, min + 8);
  EXPECT_EQ(overflow, true);
}

TEST_F(OverflowBuiltinsTest, SignedAddTest) {
  signed_t max = std::numeric_limits<signed_t>::max();
  signed_t min = std::numeric_limits<signed_t>::min();

  signed_t c;
  bool overflow;

  overflow = builtin_add_overflow<signed_t>(min, 3, &c);
  EXPECT_EQ(c, min + 3);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<signed_t>(min, 0, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<signed_t>(0, -12, &c);
  EXPECT_EQ(c, -12);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<signed_t>(min, -1, &c);
  EXPECT_EQ(c, max);
  EXPECT_EQ(overflow, true);

  overflow = builtin_add_overflow<signed_t>(max, 1, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, true);

  overflow = builtin_add_overflow<signed_t>(0, -13, &c);
  EXPECT_EQ(c, -13);
  EXPECT_EQ(overflow, false);

  overflow = builtin_add_overflow<signed_t>(-12, 13, &c);
  EXPECT_EQ(c, 1);
  EXPECT_EQ(overflow, false);
}

TEST_F(OverflowBuiltinsTest, UnsignedSubTest) {
  unsigned_t max = std::numeric_limits<unsigned_t>::max();
  unsigned_t min = std::numeric_limits<unsigned_t>::min();

  unsigned_t c;
  bool overflow;

  overflow = builtin_sub_overflow<unsigned_t>(3, 3, &c);
  EXPECT_EQ(c, 0);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<unsigned_t>(0, 0, &c);
  EXPECT_EQ(c, 0);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<unsigned_t>(min + 12, 3, &c);
  EXPECT_EQ(c, min + 9);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<unsigned_t>(min + 12, 12, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<unsigned_t>(min + 12, 13, &c);
  EXPECT_EQ(c, max);
  EXPECT_EQ(overflow, true);

  overflow = builtin_sub_overflow<unsigned_t>(min + 12, 15, &c);
  EXPECT_EQ(c, max - 2);
  EXPECT_EQ(overflow, true);
}

TEST_F(OverflowBuiltinsTest, SignedSubTest) {
  signed_t max = std::numeric_limits<signed_t>::max();
  signed_t min = std::numeric_limits<signed_t>::min();

  signed_t c;
  bool overflow;

  overflow = builtin_sub_overflow<signed_t>(min + 3, 3, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<signed_t>(min, 0, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<signed_t>(0, -12, &c);
  EXPECT_EQ(c, 12);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<signed_t>(min, 1, &c);
  EXPECT_EQ(c, max);
  EXPECT_EQ(overflow, true);

  overflow = builtin_sub_overflow<signed_t>(max, -1, &c);
  EXPECT_EQ(c, min);
  EXPECT_EQ(overflow, true);

  overflow = builtin_sub_overflow<signed_t>(0, 13, &c);
  EXPECT_EQ(c, -13);
  EXPECT_EQ(overflow, false);

  overflow = builtin_sub_overflow<signed_t>(-12, -13, &c);
  EXPECT_EQ(c, 1);
  EXPECT_EQ(overflow, false);
}

TEST_F(OverflowBuiltinsTest, UnsignedMulTest) {
  unsigned_t max = std::numeric_limits<unsigned_t>::max();

  unsigned_t c;
  bool overflow;

  overflow = builtin_mul_overflow<unsigned_t>(3, 3, &c);
  EXPECT_EQ(c, 9);
  EXPECT_EQ(overflow, false);

  overflow = builtin_mul_overflow<unsigned_t>(0, 0, &c);
  EXPECT_EQ(c, 0);
  EXPECT_EQ(overflow, false);

  overflow = builtin_mul_overflow<unsigned_t>(max, 2, &c);
  EXPECT_EQ(c, 4294967294);
  EXPECT_EQ(overflow, true);
}

TEST_F(OverflowBuiltinsTest, SignedMulTest) {
  signed_t max = std::numeric_limits<signed_t>::max();
  // signed_t min = std::numeric_limits<signed_t>::min();

  signed_t c;
  bool overflow;

  overflow = builtin_mul_overflow<signed_t>(-1, 2, &c);
  EXPECT_EQ(c, -2);
  EXPECT_EQ(overflow, false);

  overflow = builtin_mul_overflow<signed_t>(2, -4, &c);
  EXPECT_EQ(c, -8);
  EXPECT_EQ(overflow, false);

  overflow = builtin_mul_overflow<signed_t>(-4, -4, &c);
  EXPECT_EQ(c, 16);
  EXPECT_EQ(overflow, false);

  overflow = builtin_mul_overflow<signed_t>(max, -2, &c);
  EXPECT_EQ(c, 2);
  EXPECT_EQ(overflow, true);
}

}  // namespace test
}  // namespace peloton
