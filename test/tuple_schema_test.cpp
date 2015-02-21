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

#include "catalog/tuple_schema.h"

namespace nstore {

TEST(TestCase1, Test1) {

	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetFixedLengthTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetFixedLengthTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetFixedLengthTypeSize(VALUE_TYPE_TINYINT), false, true);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);

	EXPECT_EQ(column1, column2);
	EXPECT_NE(column1, column3);

	catalog::TupleSchema schema1(columns);
	std::cout << schema1.ToString();

	catalog::TupleSchema schema2(columns);
	EXPECT_EQ(schema1, schema2);

	std::vector<uint32_t> subset { 0, 2};
	catalog::TupleSchema *schema3 = catalog::TupleSchema::CopyTupleSchema(&schema2, subset);

	std::cout << schema3->ToString();

}

} // End nstore namespace

