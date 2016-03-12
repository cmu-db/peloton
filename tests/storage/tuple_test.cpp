//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple_test.cpp
//
// Identification: tests/storage/tuple_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include <memory>

#include "backend/storage/tuple.h"
#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Tests
//===--------------------------------------------------------------------===//

class TupleTests : public PelotonTest {};

TEST_F(TupleTests, BasicTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  catalog::Column column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT),
                          "C", true);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  tuple->SetValue(0, ValueFactory::GetIntegerValue(23), pool);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(45), pool);
  tuple->SetValue(2, ValueFactory::GetTinyIntValue(1), pool);

  EXPECT_EQ(tuple->GetValue(0), ValueFactory::GetIntegerValue(23));
  EXPECT_EQ(tuple->GetValue(1), ValueFactory::GetIntegerValue(45));
  EXPECT_EQ(tuple->GetValue(2), ValueFactory::GetTinyIntValue(1));

  tuple->SetValue(2, ValueFactory::GetTinyIntValue(2), pool);

  EXPECT_EQ(tuple->GetValue(2), ValueFactory::GetTinyIntValue(2));

  std::cout << (*tuple);

  delete tuple;
  delete schema;
}

TEST_F(TupleTests, VarcharTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  catalog::Column column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_VARCHAR, 25, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  tuple->SetValue(0, ValueFactory::GetIntegerValue(23), pool);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(45), pool);
  tuple->SetValue(2, ValueFactory::GetTinyIntValue(1), pool);

  Value val = ValueFactory::GetStringValue("hello hello world", pool);
  tuple->SetValue(3, val, pool);
  EXPECT_EQ(tuple->GetValue(3), val);

  std::cout << (*tuple);

  Value val2 = ValueFactory::GetStringValue("hi joy !", pool);
  tuple->SetValue(3, val2, pool);

  EXPECT_NE(tuple->GetValue(3), val);
  EXPECT_EQ(tuple->GetValue(3), val2);

  std::cout << (*tuple);

  delete tuple;
  delete schema;
}

}  // End test namespace
}  // End peloton namespace
