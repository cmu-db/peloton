//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_schema_test.cpp
//
// Identification: test/catalog/tuple_schema_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

#include "catalog/schema.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Schema Tests
//===--------------------------------------------------------------------===//

class TupleSchemaTests : public PelotonTest {};

TEST_F(TupleSchemaTests, ColumnInfoTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);

  EXPECT_EQ(column1, column2);
  EXPECT_NE(column1, column3);
}

/*
 * TupleSchemaFilteringTest() - Tests FilterSchema() which uses a set
 */
TEST_F(TupleSchemaTests, TupleSchemaFilteringTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);
  catalog::Column column4(type::TypeId::VARCHAR, 24, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema schema1(columns);
  LOG_INFO("%s", schema1.GetInfo().c_str());

  catalog::Schema schema2(columns);
  EXPECT_EQ(schema1, schema2);

  ///////////////////////////////////////////////////////////////////
  // Tests basic filterng
  ///////////////////////////////////////////////////////////////////
  
  std::vector<oid_t> subset{0, 2};
  std::unique_ptr<catalog::Schema> schema3_p(catalog::Schema::FilterSchema(&schema2, subset));
  LOG_INFO("%s", schema3_p->GetInfo().c_str());

  EXPECT_NE(schema1, (*schema3_p));
  
  ///////////////////////////////////////////////////////////////////
  // Tests out of order filtering (should not affected by order)
  ///////////////////////////////////////////////////////////////////
  
  subset = {2, 0};
  std::unique_ptr<catalog::Schema> schema4_p(catalog::Schema::FilterSchema(&schema2, subset));
  LOG_INFO("%s", schema4_p->GetInfo().c_str());

  EXPECT_EQ((*schema4_p), (*schema3_p));
  
  ///////////////////////////////////////////////////////////////////
  // Tests duplicated indices & out of bound indices
  ///////////////////////////////////////////////////////////////////
  
  subset = {666, 0, 0, 0, 0, 0, 0, 2, 0, 2, 0, 2, 100, 101};
  
  std::unique_ptr<catalog::Schema> schema5_p(catalog::Schema::FilterSchema(&schema2, subset));
  LOG_INFO("%s", schema5_p->GetInfo().c_str());

  EXPECT_EQ(schema5_p->GetColumnCount(), 2);
  EXPECT_EQ((*schema5_p), (*schema4_p));
  
  ///////////////////////////////////////////////////////////////////
  // All tests finished
  ///////////////////////////////////////////////////////////////////./t
  
  return;
}

/*
 * TupleSchemaCopyTest() - Tests CopySchema() which uses a list of indices
 */
TEST_F(TupleSchemaTests, TupleSchemaCopyTest) {
  std::vector<catalog::Column> columns;

  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);
  catalog::Column column4(type::TypeId::VARCHAR, 24, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema schema1(columns);

  ///////////////////////////////////////////////////////////////////
  // Tests basic copy
  ///////////////////////////////////////////////////////////////////

  std::vector<oid_t> subset{0, 2};
  std::unique_ptr<catalog::Schema> schema3_p(catalog::Schema::CopySchema(&schema1, subset));
  LOG_INFO("%s", schema3_p->GetInfo().c_str());

  EXPECT_NE(schema1, (*schema3_p));

  ///////////////////////////////////////////////////////////////////
  // Tests out of order copy (SHOULD affected by order)
  ///////////////////////////////////////////////////////////////////

  subset = {2, 0};
  std::unique_ptr<catalog::Schema> schema4_p(catalog::Schema::CopySchema(&schema1, subset));
  LOG_INFO("%s", schema4_p->GetInfo().c_str());

  EXPECT_NE((*schema4_p), (*schema3_p));

  ///////////////////////////////////////////////////////////////////
  // Tests duplicated column copy (avoid this in practice, but it should work)
  ///////////////////////////////////////////////////////////////////

  subset = {0, 0, 0, 0, 0, 0, 2, 0, 2, 0, 2, 1};

  std::unique_ptr<catalog::Schema> schema5_p(catalog::Schema::CopySchema(&schema1, subset));
  LOG_INFO("%s", schema5_p->GetInfo().c_str());

  EXPECT_EQ(schema5_p->GetColumnCount(), subset.size());
  EXPECT_NE((*schema5_p), (*schema4_p));
  EXPECT_NE((*schema5_p), (*schema3_p));

  ///////////////////////////////////////////////////////////////////
  // All tests finished
  ///////////////////////////////////////////////////////////////////./t

  return;
}

}  // namespace test
}  // namespace peloton
