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

#include "storage/tuple.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Tests
//===--------------------------------------------------------------------===//

class TupleTests : public PelotonTest {};

TEST_F(TupleTests, BasicTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
                          "A", true);
  catalog::Column column2(common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
                          "B", true);
  catalog::Column column3(common::Type::TINYINT, common::Type::GetTypeSize(common::Type::TINYINT),
                          "C", true);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  tuple->SetValue(0, common::ValueFactory::GetIntegerValue(23), pool);
  tuple->SetValue(1, common::ValueFactory::GetIntegerValue(45), pool);
  tuple->SetValue(2, common::ValueFactory::GetTinyIntValue(1), pool);

  common::Value val0 = (tuple->GetValue(0));
  common::Value val1 = (tuple->GetValue(1));
  common::Value val2 = (tuple->GetValue(2));

  common::Value cmp = (val0.CompareEquals(
      common::ValueFactory::GetIntegerValue(23)));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = (val1.CompareEquals(common::ValueFactory::GetIntegerValue(45)));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = (val2.CompareEquals(common::ValueFactory::GetIntegerValue(1)));
  EXPECT_TRUE(cmp.IsTrue());

  tuple->SetValue(2, common::ValueFactory::GetTinyIntValue(2), pool);

  val2 = (tuple->GetValue(2));
  cmp = (val2.CompareEquals(common::ValueFactory::GetIntegerValue(2)));
  EXPECT_TRUE(cmp.IsTrue());

  LOG_INFO("%s", tuple->GetInfo().c_str());

  delete tuple;
  delete schema;
}

TEST_F(TupleTests, VarcharTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
                          "A", true);
  catalog::Column column2(common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
                          "B", true);
  catalog::Column column3(common::Type::TINYINT, common::Type::GetTypeSize(common::Type::TINYINT),
                          "C", true);
  catalog::Column column4(common::Type::VARCHAR, 25, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  tuple->SetValue(0, common::ValueFactory::GetIntegerValue(23), pool);
  tuple->SetValue(1, common::ValueFactory::GetIntegerValue(45), pool);
  tuple->SetValue(2, common::ValueFactory::GetTinyIntValue(1), pool);

  common::Value val = common::ValueFactory::GetVarcharValue("hello hello world", pool);
  tuple->SetValue(3, val, pool);
  common::Value value3 = (tuple->GetValue(3));
  common::Value cmp = (value3.CompareEquals(val));
  EXPECT_TRUE(cmp.IsTrue());

  LOG_INFO("%s", tuple->GetInfo().c_str());

  auto val2 = common::ValueFactory::GetVarcharValue("hi joy !", pool);
  tuple->SetValue(3, val2, pool);
  value3 = (tuple->GetValue(3));
  cmp = (value3.CompareNotEquals(val));
  EXPECT_TRUE(cmp.IsTrue());
  cmp = (value3.CompareEquals(val2));
  EXPECT_TRUE(cmp.IsTrue());

  LOG_INFO("%s", tuple->GetInfo().c_str());

  delete tuple;
  delete schema;
}

}  // End test namespace
}  // End peloton namespace
