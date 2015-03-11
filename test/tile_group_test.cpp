/*-------------------------------------------------------------------------
*
* tile_group_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tile_group_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include "storage/tile_group.h"

namespace nstore {

TEST(TileGroupTests, BasicTest) {

	std::vector<catalog::ColumnInfo> columns;
	std::vector<std::string> tile_column_names;
	std::vector<std::vector<std::string> > column_names;
	std::vector<catalog::Schema*> schemas;

	// SCHEMA

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, false, false);

	columns.push_back(column1);
	columns.push_back(column2);

	catalog::Schema *schema1 = new catalog::Schema(columns);
	schemas.push_back(schema1);

	columns.clear();
	columns.push_back(column3);
	columns.push_back(column4);

	catalog::Schema *schema2 = new catalog::Schema(columns);
	schemas.push_back(schema2);

	catalog::Schema *schema = catalog::Schema::AppendSchema(schema1, schema2);

	// TUPLES

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

	// TILES

	tile_column_names.push_back("COL 1");
	tile_column_names.push_back("COL 2");

	column_names.push_back(tile_column_names);

	tile_column_names.clear();
	tile_column_names.push_back("COL 3");
	tile_column_names.push_back("COL 4");

	column_names.push_back(tile_column_names);

	// TILE GROUP

	storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(schemas, 4, column_names, true);

	tile_group->InsertTuple(*tuple1);
	tile_group->InsertTuple(*tuple2);
	tile_group->InsertTuple(*tuple1);

	std::cout << (*tile_group);

	delete tuple1;
	delete tuple2;
}

} // End nstore namespace






