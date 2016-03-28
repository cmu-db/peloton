//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_schema_test.cpp
//
// Identification: tests/catalog/tuple_schema_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/catalog/schema.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Schema Tests
//===--------------------------------------------------------------------===//

class TupleSchemaTests : public PelotonTest {};

TEST_F(TupleSchemaTests, ColumnInfoTest) {
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

TEST_F(TupleSchemaTests, TupleSchemaTest) {
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
  LOG_INFO("%s", schema1.GetInfo().c_str());

  catalog::Schema schema2(columns);
  EXPECT_EQ(schema1, schema2);

  std::vector<oid_t> subset{0, 2};
  catalog::Schema *schema3 = catalog::Schema::CopySchema(&schema2, subset);
  LOG_INFO("%s", schema3->GetInfo().c_str());

  EXPECT_NE(schema1, (*schema3));

  delete schema3;
}

}  // End test namespace
}  // End peloton namespace
