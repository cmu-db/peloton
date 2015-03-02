/*-------------------------------------------------------------------------
*
* tile_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tile_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include "../src/catalog/schema.h"

#include "storage/tile_factory.h"
#include "storage/tuple.h"
#include "common/value_factory.h"

namespace nstore {

TEST(TileTests, BasicTest) {

	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, false, false);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);
	columns.push_back(column4);

	catalog::Schema *schema = new catalog::Schema(columns);

	storage::Tuple *tuple1 = new storage::Tuple(schema, true);
	storage::Tuple *tuple2 = new storage::Tuple(schema, true);

	tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
	tuple1->SetValue(3, ValueFactory::GetStringValue("tuple 1"));

	tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
	tuple2->SetValue(3, ValueFactory::GetStringValue("tuple 2"));

	std::vector<std::string> column_names;

	column_names.push_back("COL 1");
	column_names.push_back("COL 2");
	column_names.push_back("COL 3");
	column_names.push_back("COL 4");

	storage::Tile *tile = storage::TileFactory::GetTile(schema, 3, column_names, true);

	tile->InsertTuple(0, tuple1);
	tile->InsertTuple(1, tuple2);
	tile->InsertTuple(2, tuple2);

	std::cout << (*tile);

	tile->DeleteTuple(2, false);
	tile->InsertTuple(2, tuple1);
	tile->DeleteTuple(0, false);

	std::cout << (*tile);

	delete tuple1;
	delete tuple2;
	delete tile;
}

} // End nstore namespace


