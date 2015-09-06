//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// materialization_test.cpp
//
// Identification: tests/executor/materialization_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/seq_scan_plan.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/storage/table_factory.h"
#include "backend/index/index_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"

using ::testing::NotNull;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Group Layout Tests
//===--------------------------------------------------------------------===//

void RunTest() {

  const int tuples_per_tilegroup_count = DEFAULT_TUPLES_PER_TILEGROUP;
  const int tile_group_count = 20;
  const int tuple_count = tuples_per_tilegroup_count * tile_group_count;
  const oid_t col_count = 250;
  const bool is_inlined = true;
  const bool indexes = false;

  std::vector<catalog::Column> columns;

  for(oid_t col_itr = 0 ; col_itr <= col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "FIELD" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }


  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("TEST_TABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      tuples_per_tilegroup_count));

  // PRIMARY INDEX
  if (indexes == true) {
    std::vector<oid_t> key_attrs;

    auto tuple_schema = table->GetSchema();
    catalog::Schema *key_schema;
    index::IndexMetadata *index_metadata;
    bool unique;

    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = true;

    index_metadata = new index::IndexMetadata(
        "primary_index", 123, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

    index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
    table->AddIndex(pkey_index);
  }

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();

  for (int rowid = 0; rowid < tuple_count; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple(table_schema, allocate);

    for(oid_t col_itr = 0 ; col_itr <= col_count; col_itr++) {
      auto value = ValueFactory::GetIntegerValue(populate_value + col_itr);
      tuple.SetValue(col_itr, value);
    }

    ItemPointer tuple_slot_id = table->InsertTuple(txn, &tuple);
    EXPECT_TRUE(tuple_slot_id.block != INVALID_OID);
    EXPECT_TRUE(tuple_slot_id.offset != INVALID_OID);
    txn->RecordInsert(tuple_slot_id);
  }

  txn_manager.CommitTransaction(txn);

  /////////////////////////////////////////////////////////
  // Do a seq scan with predicate on top of the table
  /////////////////////////////////////////////////////////

  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 198, 206});

  // Create and set up seq scan executor
  planner::SeqScanPlan seq_scan_node(table.get(), nullptr, column_ids);
  int expected_num_tiles = tile_group_count;

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for(auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "FIELD" + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema.release(),
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(seq_scan_executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(mat_executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(mat_executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());

    auto base_physical_tile = result_tile.get()->GetBaseTile(0);
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(mat_executor.Execute());

  txn_manager.CommitTransaction(txn);
}

TEST(TileGroupLayoutTest, ColumnLayout) {
  peloton_tilegroup_layout = PELOTON_TILEGROUP_LAYOUT_COLUMN;
  RunTest();
}

TEST(TileGroupLayoutTest, RowLayout) {
  peloton_tilegroup_layout = PELOTON_TILEGROUP_LAYOUT_ROW;
  RunTest();
}

TEST(TileGroupLayoutTest, HybridLayout) {
  peloton_tilegroup_layout = PELOTON_TILEGROUP_LAYOUT_HYBRID;
  RunTest();
}

}  // namespace test
}  // namespace peloton
