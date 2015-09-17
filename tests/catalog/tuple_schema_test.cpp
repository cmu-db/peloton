//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple_schema_test.cpp
//
// Identification: tests/catalog/tuple_schema_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "backend/catalog/schema.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Schema Tests
//===--------------------------------------------------------------------===//

TEST(TupleSchemaTests, ColumnInfoTest) {
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

  EXPECT_EQ(column1, column2);
  EXPECT_NE(column1, column3);
}

TEST(TupleSchemaTests, TupleSchemaTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  catalog::Column column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_VARCHAR, 24, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema schema1(columns);
  std::cout << schema1;

  catalog::Schema schema2(columns);
  EXPECT_EQ(schema1, schema2);

  std::vector<oid_t> subset{0, 2};
  catalog::Schema *schema3 = catalog::Schema::CopySchema(&schema2, subset);
  std::cout << (*schema3);

  EXPECT_NE(schema1, (*schema3));

  delete schema3;
}

}  // End test namespace
}  // End peloton namespace
