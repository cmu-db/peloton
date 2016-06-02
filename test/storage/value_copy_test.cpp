//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_copy_test.cpp
//
// Identification: tests/storage/value_copy_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "harness.h"

#include "backend/storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Value Copy Tests
//===--------------------------------------------------------------------===//

class ValueCopyTests : public PelotonTest {};

TEST_F(ValueCopyTests, VarcharTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_VARCHAR, 25, "D", false);

  columns.push_back(column1);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  auto pool = TestingHarness::GetInstance().GetTestingPool();

  ValueFactory::GetStringValue("hello hello world", pool);

  Value val = ValueFactory::GetStringValue("hello hello world", pool);

  Value val2 = ValueFactory::GetStringValue("hello hello world", nullptr);

  tuple->SetValue(0, val, pool);

  Value val3 = tuple->GetValue(0);

  LOG_INFO("%s", val3.GetInfo().c_str());

  delete tuple;
  delete schema;
}

}  // End test namespace
}  // End peloton namespace
