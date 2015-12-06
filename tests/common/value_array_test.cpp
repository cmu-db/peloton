//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value_array_test.cpp
//
// Identification: tests/common/value_array_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "harness.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/value_vector.h"
#include "backend/common/value_peeker.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Value Array Tests
//===--------------------------------------------------------------------===//

TEST(ValueArrayTest, BasicTest) {
    std::vector<Value> cachedStringValues;
    ValueArray array1(3);
    ValueArray array2(3);
    EXPECT_EQ(3, array1.GetSize());
    array1[0] = ValueFactory::GetBigIntValue(10);
    EXPECT_EQ(VALUE_TYPE_BIGINT, ValuePeeker::PeekValueType(array1[0]));
    EXPECT_TRUE(ValueFactory::GetBigIntValue(10).OpEquals(array1[0]).IsTrue());
    array2[0] = array1[0];
    EXPECT_EQ(VALUE_TYPE_BIGINT, ValuePeeker::PeekValueType(array2[0]));
    EXPECT_TRUE(ValueFactory::GetBigIntValue(10).OpEquals(array2[0]).IsTrue());
    EXPECT_TRUE(array1[0].OpEquals(array2[0]).IsTrue());

    cachedStringValues.push_back(ValueFactory::GetStringValue("str1"));
    array1[1] = cachedStringValues.back();
    EXPECT_EQ(VALUE_TYPE_VARCHAR, ValuePeeker::PeekValueType(array1[1]));
    cachedStringValues.push_back(ValueFactory::GetStringValue("str1"));
    EXPECT_TRUE(cachedStringValues.back().OpEquals(array1[1]).IsTrue());
    cachedStringValues.push_back(ValueFactory::GetStringValue("str2"));
    array2[1] = cachedStringValues.back();
    EXPECT_TRUE(array1[1].OpNotEquals(array2[1]).IsTrue());
    cachedStringValues.push_back(ValueFactory::GetStringValue("str2"));
    EXPECT_TRUE(cachedStringValues.back().OpEquals(array2[1]).IsTrue());

    array1[2] = ValueFactory::GetDoubleValue(0.01f);
    array2[2] = ValueFactory::GetDoubleValue(0.02f);
    EXPECT_TRUE(array1[2].OpLessThan(array2[2]).IsTrue());
    EXPECT_FALSE(array1[2].OpGreaterThan(array2[2]).IsTrue());
    EXPECT_FALSE(array1[2].OpEquals(array2[2]).IsTrue());

    EXPECT_TRUE(array1 < array2);
    EXPECT_FALSE(array1 > array2);
    EXPECT_FALSE(array1 == array2);

    array2[1].Free();

    cachedStringValues.push_back(ValueFactory::GetStringValue("str1"));
    array2[1] = cachedStringValues.back();
    array2[2] = ValueFactory::GetDoubleValue(0.01f);
    EXPECT_TRUE(array1 == array2);
    EXPECT_FALSE(array1 != array2);

    array1[1].Free();
    array2[1].Free();

}

}  // End test namespace
}  // End peloton namespace

