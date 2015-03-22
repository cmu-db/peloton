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
#include "harness.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Group Tests
//===--------------------------------------------------------------------===//

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
	txn_id_t txn_id = GetTransactionId();

	EXPECT_EQ(0, tile_group->GetActiveTupleCount());

	tile_group->InsertTuple(txn_id, tuple1);
	tile_group->InsertTuple(txn_id, tuple2);
	tile_group->InsertTuple(txn_id, tuple1);

	EXPECT_EQ(3, tile_group->GetActiveTupleCount());

	delete tuple1;
	delete tuple2;
}

void TileGroupInsert(storage::TileGroup *tile_group, catalog::Schema *schema){

	uint64_t thread_id = GetThreadId();

	storage::Tuple *tuple = new storage::Tuple(schema, true);
	txn_id_t txn_id = GetTransactionId();

	tuple->SetValue(0, ValueFactory::GetIntegerValue(1));
	tuple->SetValue(1, ValueFactory::GetIntegerValue(1));
	tuple->SetValue(2, ValueFactory::GetTinyIntValue(1));
	tuple->SetValue(3, ValueFactory::GetStringValue("thread " + std::to_string(thread_id)));

	for(int insert_itr = 0 ; insert_itr < 1000 ; insert_itr++) {
		tile_group->InsertTuple(txn_id, tuple);
	}

	delete tuple;
}

TEST(TileGroupTests, StressTest) {

	std::vector<catalog::ColumnInfo> columns;
	std::vector<std::string> tile_column_names;
	std::vector<std::vector<std::string> > column_names;
	std::vector<catalog::Schema*> schemas;

	// SCHEMA
	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 50, false, false);

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

	// TILES
	tile_column_names.push_back("COL 1");
	tile_column_names.push_back("COL 2");
	column_names.push_back(tile_column_names);

	tile_column_names.clear();
	tile_column_names.push_back("COL 3");
	tile_column_names.push_back("COL 4");
	column_names.push_back(tile_column_names);

	// TILE GROUP

	storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(schemas, 10000, column_names, true);

	LaunchParallelTest(6, TileGroupInsert, tile_group, schema);

	EXPECT_EQ(6000, tile_group->GetActiveTupleCount());
}

TEST(TileGroupTests, MVCCInsert) {

	std::vector<catalog::ColumnInfo> columns;
	std::vector<std::string> tile_column_names;
	std::vector<std::vector<std::string> > column_names;
	std::vector<catalog::Schema*> schemas;

	// SCHEMA
	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), false, true);
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 50, false, false);

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

	// TILES
	tile_column_names.push_back("COL 1");
	tile_column_names.push_back("COL 2");
	column_names.push_back(tile_column_names);

	tile_column_names.clear();
	tile_column_names.push_back("COL 3");
	tile_column_names.push_back("COL 4");
	column_names.push_back(tile_column_names);

	// TILE GROUP

	storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(schemas, 3, column_names, true);

	storage::Tuple *tuple = new storage::Tuple(schema, true);

	tuple->SetValue(0, ValueFactory::GetIntegerValue(1));
	tuple->SetValue(1, ValueFactory::GetIntegerValue(1));
	tuple->SetValue(2, ValueFactory::GetTinyIntValue(1));
	tuple->SetValue(3, ValueFactory::GetStringValue("abc"));

	id_t tuple_slot_id = INVALID_ID;

	txn_id_t txn_id1 = GetTransactionId();
	cid_t cid1 = GetCommitId();
	txn_id_t txn_id2 = GetTransactionId();
	cid_t cid2 = GetCommitId();

	tuple->SetValue(2, ValueFactory::GetIntegerValue(0));
	tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
	EXPECT_EQ(0, tuple_slot_id);

	tuple->SetValue(2, ValueFactory::GetIntegerValue(1));
	tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
	EXPECT_EQ(1, tuple_slot_id);

	tuple->SetValue(2, ValueFactory::GetIntegerValue(2));
	tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
	EXPECT_EQ(2, tuple_slot_id);

	tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
	EXPECT_EQ(INVALID_ID, tuple_slot_id);

	storage::TileGroupHeader *header = tile_group->GetHeader();

	// SELECT

	storage::Tuple *result = nullptr;
	result = tile_group->SelectTuple(txn_id2, 1, 1, cid2);

	EXPECT_EQ(result, nullptr);

	header->SetBeginCommitId(0, cid1);
	header->SetBeginCommitId(2, cid1);

	result = tile_group->SelectTuple(txn_id2, 1, 1, cid2);

	EXPECT_EQ(result, nullptr);

	result = tile_group->SelectTuple(txn_id2, 1, 0, cid2);

	EXPECT_NE(result, nullptr);

	// SCAN

	storage::Tile *scan = nullptr;
	scan = tile_group->ScanTuples(txn_id2, 1, cid2);

	EXPECT_EQ(scan->GetActiveTupleCount(), 2);

	// DELETE
	tile_group->DeleteTuple(cid2, 2);

	txn_id_t txn_id3 = GetTransactionId();
	cid_t cid3 = GetCommitId();

	scan = tile_group->ScanTuples(txn_id3, 1, cid3);

	EXPECT_EQ(scan->GetActiveTupleCount(), 1);
}

} // End test namespace
} // End nstore namespace






