//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile_test.cpp
//
// Identification: test/executor/logical_tile_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>
#include <vector>

#include "common/harness.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/logical_tile.h"

#include "executor/testing_executor_util.h"
#include "executor/logical_tile_factory.h"
#include "storage/temp_table.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "common/internal_types.h"
#include "type/value_factory.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logical Tile Tests
//===--------------------------------------------------------------------===//

class LogicalTileTests : public PelotonTest {};

TEST_F(LogicalTileTests, TempTableTest) {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  catalog::Schema *schema = new catalog::Schema(
      {TestingExecutorUtil::GetColumnInfo(0), TestingExecutorUtil::GetColumnInfo(1),
       TestingExecutorUtil::GetColumnInfo(2)});

  // Create our TempTable
  storage::TempTable table(INVALID_OID, schema, true);
  EXPECT_EQ(0, table.GetTupleCount());

  // Then shove some tuples in it
  for (int i = 0; i < tuple_count; i++) {
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table.GetSchema(), true));
    auto val1 = type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(i, 0));
    auto val2 = type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(i, 1));
    auto val3 = type::ValueFactory::GetDecimalValue(
        TestingExecutorUtil::PopulatedValue(i, 2));
    tuple->SetValue(0, val1, pool);
    tuple->SetValue(1, val2, pool);
    tuple->SetValue(2, val3, pool);
    table.InsertTuple(tuple.get());
  }
  LOG_INFO("%s", table.GetInfo().c_str());
  LOG_INFO("%s", GETINFO_SINGLE_LINE.c_str());

  // Check to see whether we can wrap a LogicalTile around it
  auto tile_group_count = table.GetTileGroupCount();
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    EXPECT_NE(nullptr, tile_group);
    std::unique_ptr<executor::LogicalTile> logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(tile_group));
    EXPECT_NE(nullptr, logical_tile);

    // Make sure that we can iterate over the LogicalTile and get
    // at our TempTable tuples
    EXPECT_NE(0, logical_tile->GetTupleCount());

    LOG_INFO("GetActiveTupleCount() = %d",
             (int)tile_group->GetActiveTupleCount());
    LOG_INFO("\n%s", tile_group->GetInfo().c_str());
    LOG_INFO("%s", peloton::GETINFO_THICK_LINE.c_str());
    LOG_INFO("%s", logical_tile->GetInfo().c_str());
  }
}

TEST_F(LogicalTileTests, TileMaterializationTest) {
  const int tuple_count = 4;
  std::shared_ptr<storage::TileGroup> tile_group(
      TestingExecutorUtil::CreateTileGroup(tuple_count));

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  // Create tuples and insert them into tile group.
  const bool allocate = true;
  storage::Tuple tuple1(schema.get(), allocate);
  storage::Tuple tuple2(schema.get(), allocate);
  auto pool = tile_group->GetTilePool(1);

  tuple1.SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1.SetValue(1, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1.SetValue(2, type::ValueFactory::GetTinyIntValue(1), pool);
  tuple1.SetValue(3, type::ValueFactory::GetVarcharValue("tuple 1"), pool);

  tuple2.SetValue(0, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2.SetValue(1, type::ValueFactory::GetIntegerValue(2), pool);
  tuple2.SetValue(2, type::ValueFactory::GetTinyIntValue(2), pool);
  tuple2.SetValue(3, type::ValueFactory::GetVarcharValue("tuple 2"), pool);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // txn_id_t txn_id = txn->GetTransactionId();

  auto tuple_id1 = tile_group->InsertTuple(&tuple1);
  auto tuple_id2 = tile_group->InsertTuple(&tuple2);
  auto tuple_id3 = tile_group->InsertTuple(&tuple1);

  ItemPointer *index_entry_ptr = nullptr;
  txn_manager.PerformInsert(
      txn, ItemPointer(tile_group->GetTileGroupId(), tuple_id1),
      index_entry_ptr);
  txn_manager.PerformInsert(
      txn, ItemPointer(tile_group->GetTileGroupId(), tuple_id2),
      index_entry_ptr);
  txn_manager.PerformInsert(
      txn, ItemPointer(tile_group->GetTileGroupId(), tuple_id3),
      index_entry_ptr);

  txn_manager.CommitTransaction(txn);

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (1 BASE TILE)
  ////////////////////////////////////////////////////////////////

  // Don't transfer ownership of any base tile to logical tile.
  auto base_tile_ref = tile_group->GetTileReference(1);

  std::vector<oid_t> position_list1 = {0, 1};
  std::vector<oid_t> position_list2 = {0, 1};

  std::unique_ptr<executor::LogicalTile> logical_tile(
      executor::LogicalTileFactory::GetTile());

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));

  PL_ASSERT(tile_schemas.size() == 2);
  catalog::Schema *schema1 = &tile_schemas[0];
  catalog::Schema *schema2 = &tile_schemas[1];
  oid_t column_count = schema2->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    logical_tile->AddColumn(base_tile_ref, column_itr, column_itr);
  }

  LOG_TRACE("%s", logical_tile->GetInfo().c_str());

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (2 BASE TILE)
  ////////////////////////////////////////////////////////////////

  logical_tile.reset(executor::LogicalTileFactory::GetTile());

  auto base_tile_ref1 = tile_group->GetTileReference(0);
  auto base_tile_ref2 = tile_group->GetTileReference(1);

  position_list1 = {0, 1};
  position_list2 = {0, 1};
  std::vector<oid_t> position_list3 = {0, 1};
  std::vector<oid_t> position_list4 = {0, 1};

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));
  logical_tile->AddPositionList(std::move(position_list3));
  logical_tile->AddPositionList(std::move(position_list4));

  oid_t column_count1 = schema1->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count1; column_itr++) {
    logical_tile->AddColumn(base_tile_ref1, column_itr, column_itr);
  }

  oid_t column_count2 = schema2->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count2; column_itr++) {
    logical_tile->AddColumn(base_tile_ref2, column_itr,
                            column_count1 + column_itr);
  }

  LOG_TRACE("%s", logical_tile->GetInfo().c_str());
}

}  // namespace test
}  // namespace peloton
