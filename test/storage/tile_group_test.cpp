//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_test.cpp
//
// Identification: test/storage/tile_group_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

#include "type/value_factory.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tile.h"
#include "storage/tuple.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Group Tests
//===--------------------------------------------------------------------===//

class TileGroupTests : public PelotonTest {};

TEST_F(TileGroupTests, BasicTest) {
  std::vector<catalog::Column> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Schema> schemas;

  // SCHEMA

  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);
  catalog::Column column4(type::TypeId::VARCHAR, 25, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);

  std::unique_ptr<catalog::Schema> schema1(new catalog::Schema(columns));
  schemas.push_back(*schema1);

  columns.clear();
  columns.push_back(column3);
  columns.push_back(column4);

  std::unique_ptr<catalog::Schema> schema2(new catalog::Schema(columns));
  schemas.push_back(*schema2);

  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchema(schema1.get(), schema2.get()));

  // TILES

  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");

  column_names.push_back(tile_column_names);

  tile_column_names.clear();
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");

  column_names.push_back(tile_column_names);

  // TILE GROUP
  std::map<oid_t, std::pair<oid_t, oid_t>> column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  std::shared_ptr<storage::TileGroup> tile_group(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), nullptr, schemas,
          column_map, 4));
  catalog::Manager::GetInstance().AddTileGroup(tile_group->GetTileGroupId(),
                                               tile_group);

  // TUPLES

  std::unique_ptr<storage::Tuple> tuple1(
      new storage::Tuple(schema.get(), true));
  std::unique_ptr<storage::Tuple> tuple2(
      new storage::Tuple(schema.get(), true));
  auto pool = tile_group->GetTilePool(1);

  tuple1->SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1->SetValue(1, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);
  tuple1->SetValue(3, type::ValueFactory::GetVarcharValue("tuple 1"), pool);

  tuple2->SetValue(0, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2->SetValue(1, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2->SetValue(2, type::ValueFactory::GetTinyIntValue(2), pool);
  tuple2->SetValue(3, type::ValueFactory::GetVarcharValue("tuple 2"), pool);

  // TRANSACTION

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  EXPECT_EQ(0, tile_group->GetActiveTupleCount());

  auto txn = txn_manager.BeginTransaction();

  auto tuple_slot = tile_group->InsertTuple(tuple1.get());
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_slot));

  tuple_slot = tile_group->InsertTuple(tuple2.get());
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_slot));

  tuple_slot = tile_group->InsertTuple(tuple1.get());
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_slot));

  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(3, tile_group->GetActiveTupleCount());
}

void TileGroupInsert(std::shared_ptr<storage::TileGroup> tile_group,
                     catalog::Schema *schema,
                     UNUSED_ATTRIBUTE uint64_t thread_itr) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto pool = tile_group->GetTilePool(1);

  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), pool);
  tuple->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);
  tuple->SetValue(
      3, type::ValueFactory::GetVarcharValue("thread " + std::to_string(thread_id)),
      pool);

  for (int insert_itr = 0; insert_itr < 1000; insert_itr++) {
    ItemPointer *index_entry_ptr = nullptr;
    auto tuple_slot = tile_group->InsertTuple(tuple.get());
    txn_manager.PerformInsert(txn,
        ItemPointer(tile_group->GetTileGroupId(), tuple_slot), index_entry_ptr);
  }

  txn_manager.CommitTransaction(txn);
}

TEST_F(TileGroupTests, StressTest) {
  std::vector<catalog::Column> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Schema> schemas;

  // SCHEMA
  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);
  catalog::Column column4(type::TypeId::VARCHAR, 50, "D", false);

  columns.push_back(column1);
  columns.push_back(column2);

  std::unique_ptr<catalog::Schema> schema1(new catalog::Schema(columns));
  schemas.push_back(*schema1);

  columns.clear();
  columns.push_back(column3);
  columns.push_back(column4);

  std::unique_ptr<catalog::Schema> schema2(new catalog::Schema(columns));
  schemas.push_back(*schema2);

  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchema(schema1.get(), schema2.get()));

  // TILES
  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");
  column_names.push_back(tile_column_names);

  tile_column_names.clear();
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");
  column_names.push_back(tile_column_names);

  // TILE GROUP

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  std::shared_ptr<storage::TileGroup> tile_group(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), nullptr, schemas,
          column_map, 10000));
  catalog::Manager::GetInstance().AddTileGroup(tile_group->GetTileGroupId(),
                                               tile_group);

  LaunchParallelTest(6, TileGroupInsert, tile_group, schema.get());

  EXPECT_EQ(6000, tile_group->GetActiveTupleCount());
}

// TEST_F(TileGroupTests, MVCCInsert) {
//  std::vector<catalog::Column> columns;
//  std::vector<std::string> tile_column_names;
//  std::vector<std::vector<std::string>> column_names;
//  std::vector<catalog::Schema> schemas;
//
//  // SCHEMA
//  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
//                          "A", true);
//  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
//                          "B", true);
//  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
//                          "C", true);
//  catalog::Column column4(type::TypeId::VARCHAR, 50, "D", false);
//
//  columns.push_back(column1);
//  columns.push_back(column2);
//
//  catalog::Schema *schema1 = new catalog::Schema(columns);
//  schemas.push_back(*schema1);
//
//  columns.clear();
//  columns.push_back(column3);
//  columns.push_back(column4);
//
//  catalog::Schema *schema2 = new catalog::Schema(columns);
//  schemas.push_back(*schema2);
//
//  catalog::Schema *schema = catalog::Schema::AppendSchema(schema1, schema2);
//
//  // TILES
//  tile_column_names.push_back("COL 1");
//  tile_column_names.push_back("COL 2");
//  column_names.push_back(tile_column_names);
//
//  tile_column_names.clear();
//  tile_column_names.push_back("COL 3");
//  tile_column_names.push_back("COL 4");
//  column_names.push_back(tile_column_names);
//
//  // TILE GROUP
//  std::map<oid_t, std::pair<oid_t, oid_t>> column_map;
//  column_map[0] = std::make_pair(0, 0);
//  column_map[1] = std::make_pair(0, 1);
//  column_map[2] = std::make_pair(1, 0);
//  column_map[3] = std::make_pair(1, 1);
//
//  std::shared_ptr<storage::TileGroup> tile_group =
//  storage::TileGroupFactory::GetTileGroup(
//      INVALID_OID, INVALID_OID,
//      TestingHarness::GetInstance().GetNextTileGroupId(), nullptr, schemas,
//      column_map, 3);
//  catalog::Manager::GetInstance().AddTileGroup(tile_group->GetTileGroupId(),
//  tile_group);
//
//  storage::Tuple *tuple = new storage::Tuple(schema, true);
//  auto pool = tile_group->GetTilePool(1);
//
//  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
//  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1), pool);
//  tuple->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);
//  tuple->SetValue(3, type::ValueFactory::GetVarcharValue("abc"), pool);
//
//  oid_t tuple_slot_id = INVALID_OID;
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  txn_id_t txn_id1 = txn->GetTransactionId();
//  cid_t cid1 = txn->GetBeginCommitId();
//
//  tuple->SetValue(2, type::ValueFactory::GetIntegerValue(0), pool);
//  tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
//  txn_manager.RecordInsert(tile_group->GetTileGroupId(), tuple_slot_id);
//  EXPECT_EQ(0, tuple_slot_id);
//
//  tuple->SetValue(2, type::ValueFactory::GetIntegerValue(1), pool);
//  tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
//  txn_manager.RecordInsert(tile_group->GetTileGroupId(), tuple_slot_id);
//  EXPECT_EQ(1, tuple_slot_id);
//
//  tuple->SetValue(2, type::ValueFactory::GetIntegerValue(2), pool);
//  tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
//  txn_manager.RecordInsert(tile_group->GetTileGroupId(), tuple_slot_id);
//  EXPECT_EQ(2, tuple_slot_id);
//
//  tuple_slot_id = tile_group->InsertTuple(txn_id1, tuple);
//  EXPECT_EQ(INVALID_OID, tuple_slot_id);
//
//  storage::TileGroupHeader *header = tile_group->GetHeader();
//
//  // SELECT
//
//  header->SetBeginCommitId(0, cid1);
//  header->SetBeginCommitId(2, cid1);
//
//  txn_manager.CommitTransaction();
//
//  // DELETE
//  auto txn2 = txn_manager.BeginTransaction();
//  txn_id_t tid2 = txn2->GetTransactionId();
//  cid_t lcid2 = txn2->GetBeginCommitId();
//
//  tile_group->DeleteTuple(tid2, 2, lcid2);
//
//  txn_manager.CommitTransaction();
//
//  delete tuple;
//  delete schema;
//
//  delete schema1;
//  delete schema2;
//}

TEST_F(TileGroupTests, TileCopyTest) {
  std::vector<catalog::Column> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Schema> schemas;

  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  catalog::Column column2(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "B", true);
  catalog::Column column3(type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
                          "C", true);
  catalog::Column column4(type::TypeId::VARCHAR, 25, "D", false);
  catalog::Column column5(type::TypeId::VARCHAR, 25, "E", false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);
  columns.push_back(column5);

  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));
  schemas.push_back(*schema);

  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");
  tile_column_names.push_back("COL 5");

  column_names.push_back(tile_column_names);

  const int tuple_count = 4;
  storage::column_map_type column_map;

  // default column map
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    column_map[col_itr] = std::make_pair(0, col_itr);
  }

  std::shared_ptr<storage::TileGroup> tile_group(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), nullptr, schemas,
          column_map, tuple_count));
  catalog::Manager::GetInstance().AddTileGroup(tile_group->GetTileGroupId(),
                                               tile_group);

  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  std::unique_ptr<storage::Tile> tile(storage::TileFactory::GetTile(
      BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      tile_group_header, *schema, nullptr, tuple_count));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // txn_id_t txn_id1 = txn->GetTransactionId();
  oid_t tuple_slot_id = INVALID_OID;
  auto pool = tile->GetPool();

  std::unique_ptr<storage::Tuple> tuple1(
      new storage::Tuple(schema.get(), true));
  std::unique_ptr<storage::Tuple> tuple2(
      new storage::Tuple(schema.get(), true));
  std::unique_ptr<storage::Tuple> tuple3(
      new storage::Tuple(schema.get(), true));

  tuple1->SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1->SetValue(1, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1->SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);
  tuple1->SetValue(3, type::ValueFactory::GetVarcharValue("vivek sengupta"), pool);
  tuple1->SetValue(4, type::ValueFactory::GetVarcharValue("vivek sengupta again"),
                   pool);

  tuple2->SetValue(0, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2->SetValue(1, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2->SetValue(2, type::ValueFactory::GetTinyIntValue(2), pool);
  tuple2->SetValue(3, type::ValueFactory::GetVarcharValue("ming fang"), pool);
  tuple2->SetValue(4, type::ValueFactory::GetVarcharValue("ming fang again"), pool);

  tuple3->SetValue(0, type::ValueFactory::GetIntegerValue(3), pool);
  tuple3->SetValue(1, type::ValueFactory::GetIntegerValue(3), pool);
  tuple3->SetValue(2, type::ValueFactory::GetTinyIntValue(3), pool);
  tuple3->SetValue(3, type::ValueFactory::GetVarcharValue("jinwoong kim"), pool);
  tuple3->SetValue(4, type::ValueFactory::GetVarcharValue("jinwoong kim again"), pool);

  tile->InsertTuple(0, tuple1.get());
  tile->InsertTuple(1, tuple2.get());
  tile->InsertTuple(2, tuple3.get());

  tuple_slot_id = tile_group->InsertTuple(tuple1.get());
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_slot_id));
  EXPECT_EQ(0, tuple_slot_id);
  tuple_slot_id = tile_group->InsertTuple(tuple2.get());
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_slot_id));
  EXPECT_EQ(1, tuple_slot_id);
  tuple_slot_id = tile_group->InsertTuple(tuple3.get());
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_slot_id));
  EXPECT_EQ(2, tuple_slot_id);

  txn_manager.CommitTransaction(txn);

  LOG_INFO(" Original Tile Details ...");
  LOG_INFO("%s", tile->GetInfo().c_str());

  const catalog::Schema *old_schema = tile->GetSchema();
  const catalog::Schema *new_schema = old_schema;
  std::unique_ptr<storage::Tile> new_tile(tile->CopyTile(BackendType::MM));

  LOG_INFO(" Copied Tile Details ...");
  LOG_INFO("%s", new_tile->GetInfo().c_str());

  /*
   * Test for equality of old and new tile data
   * 1. Check whether old and new pool are different
   * 2. Check whether old and new information are same for all the uninlined
   * columns
   * 	  expect the pointers to the Varlen objects, which should be different
   * 	  since the actual data should reside in different pools for the two
   * tiles
   */

  // 1. Pools
  bool intended_behavior = true;
  auto old_pool = tile->GetPool();
  auto new_pool = new_tile->GetPool();

  int is_pool_same = old_pool == new_pool;
  if (is_pool_same) intended_behavior = false;

  // 2. Information (Value objects, lengths, pointers to Varlen objects, stored
  // data)
  int uninlined_col_index;
  size_t uninlined_col_object_len, new_uninlined_col_object_len;
  char *uninlined_col_object_ptr, *new_uninlined_col_object_ptr;

  int new_tile_uninlined_col_cnt = new_schema->GetUninlinedColumnCount();

  // Iterate over all the uninlined columns in the tile
  for (int col_itr = 0; col_itr < new_tile_uninlined_col_cnt; col_itr++) {
    uninlined_col_index = new_schema->GetUninlinedColumn(col_itr);
    uint16_t new_tile_active_tuple_count = new_tile->GetActiveTupleCount();

    // Iterate over all the tuples for the current uninlined column in the tile
    for (int tup_itr = 0; tup_itr < new_tile_active_tuple_count; tup_itr++) {
      //Value uninlined_col_value, new_uninlined_col_value;

      type::Value uninlined_col_value = (
          tile->GetValue(tup_itr, uninlined_col_index));
      uninlined_col_object_len = uninlined_col_value.GetLength();
      uninlined_col_object_ptr = (char *)uninlined_col_value.GetData();
      std::string uninlined_varchar_str(
          reinterpret_cast<char const *>(uninlined_col_object_ptr),
          uninlined_col_object_len);

      type::Value new_uninlined_col_value = (
          new_tile->GetValue(tup_itr, uninlined_col_index));
      new_uninlined_col_object_len = new_uninlined_col_value.GetLength();
      new_uninlined_col_object_ptr = (char *)new_uninlined_col_value.GetData();
      std::string new_uninlined_varchar_str(
          reinterpret_cast<char const *>(new_uninlined_col_object_ptr),
          new_uninlined_col_object_len);

      // Compare original and copied tile details for current tuple
      type::Value cmp = type::ValueFactory::GetBooleanValue((uninlined_col_value.CompareNotEquals(
          new_uninlined_col_value)));
      int is_value_not_same = cmp.IsTrue();
//      int is_length_same = uninlined_col_object_len == uninlined_col_object_len;
      int is_length_same = true;
      int is_pointer_same =
          uninlined_col_object_ptr == new_uninlined_col_object_ptr;
      int is_data_same = std::strcmp(uninlined_varchar_str.c_str(),
                                     new_uninlined_varchar_str.c_str());

      // Break if there is any mismatch
      if (is_value_not_same || !is_length_same || is_pointer_same || is_data_same) {
        intended_behavior = false;
        break;
      }
    }

    // Break if intended_behavior flag has changed to false in the inner loop
    if (!intended_behavior) break;
  }

  // At the end of all the checks, intended_behavior is expected to be true
  EXPECT_TRUE(intended_behavior);
}

}  // namespace test
}  // namespace peloton
