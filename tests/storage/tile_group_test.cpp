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

#include "backend/concurrency/transaction.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_factory.h"

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Group Tests
//===--------------------------------------------------------------------===//

TEST(TileGroupTests, BasicTest) {

    std::vector<catalog::ColumnInfo> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string> > column_names;
    std::vector<catalog::Schema> schemas;

    // SCHEMA

    catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
    catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", true);
    catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", true);
    catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, "D", false);

    columns.push_back(column1);
    columns.push_back(column2);

    catalog::Schema *schema1 = new catalog::Schema(columns);
    schemas.push_back(*schema1);

    columns.clear();
    columns.push_back(column3);
    columns.push_back(column4);

    catalog::Schema *schema2 = new catalog::Schema(columns);
    schemas.push_back(*schema2);

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
    storage::AbstractBackend *backend = new storage::VMBackend();

    storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(INVALID_OID, INVALID_OID, INVALID_OID,
                                     nullptr, backend, schemas, 4);

    // TUPLES

    storage::Tuple *tuple1 = new storage::Tuple(schema, true);
    storage::Tuple *tuple2 = new storage::Tuple(schema, true);

    tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
    tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
    tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
    tuple1->SetValue(3, ValueFactory::GetStringValue("tuple 1", tile_group->GetTilePool(1)));

    tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
    tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
    tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
    tuple2->SetValue(3, ValueFactory::GetStringValue("tuple 2", tile_group->GetTilePool(1)));

    // TRANSACTION

    auto& txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    txn_id_t txn_id = txn->GetTransactionId();

    EXPECT_EQ(0, tile_group->GetActiveTupleCount());

    auto tuple_slot = tile_group->InsertTuple(txn_id, tuple1);
    tile_group->CommitInsertedTuple(txn_id, tuple_slot);

    tuple_slot =tile_group->InsertTuple(txn_id, tuple2);
    tile_group->CommitInsertedTuple(txn_id, tuple_slot);

    tuple_slot = tile_group->InsertTuple(txn_id, tuple1);
    tile_group->CommitInsertedTuple(txn_id, tuple_slot);

    EXPECT_EQ(3, tile_group->GetActiveTupleCount());

    storage::TileGroupHeader *header = tile_group->GetHeader();

    ItemPointer item(50, 60);

    header->SetPrevItemPointer(2, item);

    txn_manager.CommitTransaction(txn);
    txn_manager.EndTransaction(txn);

    delete tuple1;
    delete tuple2;
    delete schema;

    delete tile_group;
    delete backend;
    delete schema1;
    delete schema2;
}

void TileGroupInsert(storage::TileGroup *tile_group, catalog::Schema *schema) {

    uint64_t thread_id = GetThreadId();

    storage::Tuple *tuple = new storage::Tuple(schema, true);
    auto& txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    txn_id_t txn_id = txn->GetTransactionId();

    tuple->SetValue(0, ValueFactory::GetIntegerValue(1));
    tuple->SetValue(1, ValueFactory::GetIntegerValue(1));
    tuple->SetValue(2, ValueFactory::GetTinyIntValue(1));
    tuple->SetValue(3, ValueFactory::GetStringValue("thread " + std::to_string(thread_id), tile_group->GetTilePool(1)));

    for(int insert_itr = 0 ; insert_itr < 1000 ; insert_itr++) {
        auto tuple_slot = tile_group->InsertTuple(txn_id, tuple);
        tile_group->CommitInsertedTuple(txn_id, tuple_slot);
    }

    txn_manager.CommitTransaction(txn);
    txn_manager.EndTransaction(txn);

    delete tuple;
}

TEST(TileGroupTests, StressTest) {

    std::vector<catalog::ColumnInfo> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string> > column_names;
    std::vector<catalog::Schema> schemas;

    // SCHEMA
    catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
    catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", true);
    catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", true);
    catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 50, "D", false);

    columns.push_back(column1);
    columns.push_back(column2);

    catalog::Schema *schema1 = new catalog::Schema(columns);
    schemas.push_back(*schema1);

    columns.clear();
    columns.push_back(column3);
    columns.push_back(column4);

    catalog::Schema *schema2 = new catalog::Schema(columns);
    schemas.push_back(*schema2);

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

    storage::AbstractBackend *backend = new storage::VMBackend();

    storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(INVALID_OID, INVALID_OID, INVALID_OID,
                                     nullptr, backend, schemas, 10000);

    LaunchParallelTest(6, TileGroupInsert, tile_group, schema);

    EXPECT_EQ(6000, tile_group->GetActiveTupleCount());

    delete tile_group;
    delete backend;
    delete schema1;
    delete schema2;
    delete schema;
}

TEST(TileGroupTests, MVCCInsert) {

    std::vector<catalog::ColumnInfo> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string> > column_names;
    std::vector<catalog::Schema> schemas;

    // SCHEMA
    catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
    catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", true);
    catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", true);
    catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 50, "D", false);

    columns.push_back(column1);
    columns.push_back(column2);

    catalog::Schema *schema1 = new catalog::Schema(columns);
    schemas.push_back(*schema1);

    columns.clear();
    columns.push_back(column3);
    columns.push_back(column4);

    catalog::Schema *schema2 = new catalog::Schema(columns);
    schemas.push_back(*schema2);

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
    storage::AbstractBackend *backend = new storage::VMBackend();

    storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(INVALID_OID, INVALID_OID, INVALID_OID,
                                     nullptr, backend, schemas, 3);

    storage::Tuple *tuple = new storage::Tuple(schema, true);

    tuple->SetValue(0, ValueFactory::GetIntegerValue(1));
    tuple->SetValue(1, ValueFactory::GetIntegerValue(1));
    tuple->SetValue(2, ValueFactory::GetTinyIntValue(1));
    tuple->SetValue(3, ValueFactory::GetStringValue("abc", tile_group->GetTilePool(1)));

    oid_t tuple_slot_id = INVALID_OID;

    auto& txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    txn_id_t txn_id1 = txn->GetTransactionId();
    cid_t cid1 = txn->GetLastCommitId();

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
    EXPECT_EQ(INVALID_OID, tuple_slot_id);

    storage::TileGroupHeader *header = tile_group->GetHeader();

    // SELECT

    storage::Tuple *result = nullptr;
    result = tile_group->SelectTuple(1, 1);
    EXPECT_NE(result, nullptr);
    delete result;

    header->SetBeginCommitId(0, cid1);
    header->SetBeginCommitId(2, cid1);

    result = tile_group->SelectTuple(1, 1);
    EXPECT_NE(result, nullptr);
    delete result;

    result = tile_group->SelectTuple(1, 0);
    EXPECT_NE(result, nullptr);
    delete result;

    txn_manager.CommitTransaction(txn);
    txn_manager.EndTransaction(txn);

    // DELETE
    auto txn2 = txn_manager.BeginTransaction();
    cid_t cid2 = txn2->GetLastCommitId();

    tile_group->DeleteTuple(cid2, 2);

    txn_manager.CommitTransaction(txn2);
    txn_manager.EndTransaction(txn2);

    delete tuple;
    delete schema;
    delete tile_group;
    delete backend;

    delete schema1;
    delete schema2;
}

} // End test namespace
} // End peloton namespace






