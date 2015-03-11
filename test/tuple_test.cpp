/*-------------------------------------------------------------------------
*
* tuple_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tuple_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include <memory>

#include "storage/tuple.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Tuple Tests
//===--------------------------------------------------------------------===//

TEST(TupleTests, BasicTest) {

	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);

	catalog::Schema *schema(new catalog::Schema(columns));

	storage::Tuple *tuple(new storage::Tuple(schema, true));

	tuple->SetValue(0, ValueFactory::GetIntegerValue(23));
	tuple->SetValue(1, ValueFactory::GetIntegerValue(45));
	tuple->SetValue(2, ValueFactory::GetTinyIntValue(1));

	EXPECT_EQ(tuple->GetValue(0), ValueFactory::GetIntegerValue(23));
	EXPECT_EQ(tuple->GetValue(1), ValueFactory::GetIntegerValue(45));
	EXPECT_EQ(tuple->GetValue(2), ValueFactory::GetTinyIntValue(1));

	tuple->SetValue(2, ValueFactory::GetTinyIntValue(2));

	EXPECT_EQ(tuple->GetValue(2), ValueFactory::GetTinyIntValue(2));

	std::cout << (*tuple);

	delete tuple;
	delete schema;
}

TEST(TupleTests, VarcharTest) {
	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, false, false);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);
	columns.push_back(column4);

	catalog::Schema *schema(new catalog::Schema(columns));

	storage::Tuple *tuple(new storage::Tuple(schema, true));

	tuple->SetValue(0, ValueFactory::GetIntegerValue(23));
	tuple->SetValue(1, ValueFactory::GetIntegerValue(45));
	tuple->SetValue(2, ValueFactory::GetTinyIntValue(1));
	tuple->SetValue(3, ValueFactory::GetStringValue("hello hello world"));
	EXPECT_EQ(tuple->GetValue(3), ValueFactory::GetStringValue("hello hello world"));

	std::cout << (*tuple);

	tuple->SetValue(3, ValueFactory::GetStringValue("hi joy !"));
	EXPECT_EQ(tuple->GetValue(3), ValueFactory::GetStringValue("hi joy !"));
	EXPECT_NE(tuple->GetValue(3), ValueFactory::GetStringValue("hello hello world"));

	std::cout << (*tuple);

	delete tuple;
	delete schema;
}

} // End test namespace
} // End nstore namespace

