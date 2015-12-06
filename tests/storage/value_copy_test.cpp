//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value_copy_test.cpp
//
// Identification: tests/storage/value_copy_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <memory>

#include "backend/storage/tuple.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Value Copy Tests
//===--------------------------------------------------------------------===//

TEST(ValueCopyTests, VarcharTest) {

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_VARCHAR, 25, "D", false);

  columns.push_back(column1);

  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  auto pool = new peloton::VarlenPool();

  ValueFactory::GetStringValue("hello hello world", pool);

  Value val = ValueFactory::GetStringValue("hello hello world", pool);

  tuple->SetValue(0, val);

  std::cout << "Going to get value \n";

  std::cout << tuple->GetValue(0);

  delete tuple;
  delete schema;

  delete pool;

}

}  // End test namespace
}  // End peloton namespace
