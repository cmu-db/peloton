/*-------------------------------------------------------------------------
*
* tuple_schema_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tuple_schema_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include "backend/catalog/schema.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Schema Tests
//===--------------------------------------------------------------------===//

TEST(TupleSchemaTests, ColumnInfoTest) {

	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", false, true);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);

	EXPECT_EQ(column1, column2);
	EXPECT_NE(column1, column3);
}

TEST(TupleSchemaTests, TupleSchemaTest) {
	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", false, true);
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 24, "D", false, false);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);
	columns.push_back(column4);

	catalog::Schema schema1(columns);
	std::cout << schema1;

	catalog::Schema schema2(columns);
	EXPECT_EQ(schema1, schema2);

	std::vector<oid_t> subset { 0, 2};
	catalog::Schema *schema3 = catalog::Schema::CopySchema(&schema2, subset);
	std::cout << (*schema3);

	EXPECT_NE(schema1, (*schema3));

	delete schema3;
}

} // End test namespace
} // End peloton namespace

