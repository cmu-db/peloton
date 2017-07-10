//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_test.cpp
//
// Identification: test/storage/tuple_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <memory>

#include "common/harness.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Tests
//===--------------------------------------------------------------------===//

class TupleTests : public PelotonTest {};

TEST_F(TupleTests, BasicTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "A",
                          true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "B",
                          true);
  catalog::Column column3(type::TypeId::TINYINT,
                          type::Type::GetTypeSize(type::TypeId::TINYINT), "C",
                          true);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(23), pool);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(45), pool);
  tuple->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);

  type::Value val0 = (tuple->GetValue(0));
  type::Value val1 = (tuple->GetValue(1));
  type::Value val2 = (tuple->GetValue(2));

  type::Value cmp = type::ValueFactory::GetBooleanValue(
      (val0.CompareEquals(type::ValueFactory::GetIntegerValue(23))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val1.CompareEquals(type::ValueFactory::GetIntegerValue(45))));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue(
      (val2.CompareEquals(type::ValueFactory::GetIntegerValue(1))));
  EXPECT_TRUE(cmp.IsTrue());

  tuple->SetValue(2, type::ValueFactory::GetTinyIntValue(2), pool);

  val2 = (tuple->GetValue(2));
  cmp = type::ValueFactory::GetBooleanValue(
      (val2.CompareEquals(type::ValueFactory::GetIntegerValue(2))));
  EXPECT_TRUE(cmp.IsTrue());

  // Make sure that our tuple tells us the right estimated size
  // for uninlined attributes
  EXPECT_EQ(0, tuple->GetUninlinedMemorySize());

  LOG_TRACE("%s", tuple->GetInfo().c_str());

  delete tuple;
  delete schema;
}

TEST_F(TupleTests, VarcharTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "A",
                          true);
  catalog::Column column2(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "B",
                          true);
  catalog::Column column3(type::TypeId::TINYINT,
                          type::Type::GetTypeSize(type::TypeId::TINYINT), "C",
                          true);
  catalog::Column column4(type::TypeId::VARCHAR, 25, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(23), pool);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(45), pool);
  tuple->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);

  type::Value val = type::ValueFactory::GetVarcharValue(
      (std::string) "hello hello world", pool);
  tuple->SetValue(3, val, pool);
  type::Value value3 = (tuple->GetValue(3));
  type::Value cmp =
      type::ValueFactory::GetBooleanValue((value3.CompareEquals(val)));
  EXPECT_TRUE(cmp.IsTrue());

  LOG_TRACE("%s", tuple->GetInfo().c_str());

  auto val2 =
      type::ValueFactory::GetVarcharValue((std::string) "hi joy !", pool);
  tuple->SetValue(3, val2, pool);
  value3 = (tuple->GetValue(3));
  cmp = type::ValueFactory::GetBooleanValue((value3.CompareNotEquals(val)));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue((value3.CompareEquals(val2)));
  EXPECT_TRUE(cmp.IsTrue());

  LOG_TRACE("%s", tuple->GetInfo().c_str());

  // test if VARCHAR length is enforced
  auto val3 = type::ValueFactory::GetVarcharValue((std::string) "this is a very long string", pool);
  try {
    tuple->SetValue(3, val3, pool);
  }
  catch (peloton::ValueOutOfRangeException e) {}
  value3 = (tuple->GetValue(3));
  cmp = type::ValueFactory::GetBooleanValue((value3.CompareNotEquals(val3)));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = type::ValueFactory::GetBooleanValue((value3.CompareEquals(val2)));
  EXPECT_TRUE(cmp.IsTrue());

  LOG_TRACE("%s", tuple->GetInfo().c_str());

  // Make sure that our tuple tells us the right estimated size
  size_t expected_size = sizeof(int32_t) + value3.GetLength();
  EXPECT_EQ(expected_size, tuple->GetUninlinedMemorySize());

  delete tuple;
  delete schema;
}

}  // End test namespace
}  // End peloton namespace
