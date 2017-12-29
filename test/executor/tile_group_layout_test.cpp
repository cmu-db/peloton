//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_layout_test.cpp
//
// Identification: test/executor/tile_group_layout_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <ctime>

#include "executor/testing_executor_util.h"
#include "common/harness.h"

#include "planner/abstract_plan.h"
#include "planner/materialization_plan.h"
#include "planner/seq_scan_plan.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/timer.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/data_table.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/seq_scan_executor.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "index/index_factory.h"

#include "executor/mock_executor.h"

using ::testing::NotNull;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Group Layout Tests
//===--------------------------------------------------------------------===//

class TileGroupLayoutTests : public PelotonTest {};

void ExecuteTileGroupTest(peloton::LayoutType layout_type) {
  const int tuples_per_tilegroup_count = 10;
  const int tile_group_count = 5;
  const int tuple_count = tuples_per_tilegroup_count * tile_group_count;
  const oid_t col_count = 250;
  const bool is_inlined = true;
  const bool indexes = false;

  std::vector<catalog::Column> columns;

  for (oid_t col_itr = 0; col_itr <= col_count; col_itr++) {
    auto column =
        catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                        "FIELD" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("TEST_TABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  bool is_catalog = false;
  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      tuples_per_tilegroup_count, own_schema, adapt_table, is_catalog, layout_type));

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
        "primary_index", 123, INVALID_OID, INVALID_OID, IndexType::BWTREE,
        IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
        unique);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetIndex(index_metadata));
    table->AddIndex(pkey_index);
  }

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int rowid = 0; rowid < tuple_count; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple(table_schema, allocate);

    for (oid_t col_itr = 0; col_itr <= col_count; col_itr++) {
      auto value = type::ValueFactory::GetIntegerValue(populate_value + col_itr);
      tuple.SetValue(col_itr, value, testing_pool);
    }

    ItemPointer *index_entry_ptr = nullptr;
    ItemPointer tuple_slot_id =
        table->InsertTuple(&tuple, txn, &index_entry_ptr);

    EXPECT_TRUE(tuple_slot_id.block != INVALID_OID);
    EXPECT_TRUE(tuple_slot_id.offset != INVALID_OID);

    txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
  }

  txn_manager.CommitTransaction(txn);

  /////////////////////////////////////////////////////////
  // Do a seq scan with predicate on top of the table
  /////////////////////////////////////////////////////////

  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  // std::vector<oid_t> column_ids;
  // for(oid_t col_itr = 0 ; col_itr <= 200; col_itr++) {
  //  column_ids.push_back(col_itr);
  //}
  std::vector<oid_t> column_ids({198, 206});

  // Create and set up seq scan executor
  planner::SeqScanPlan seq_scan_node(table.get(), nullptr, column_ids);
  int expected_num_tiles = tile_group_count;

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for (auto column_id : column_ids) {
    auto column =
        catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                        "FIELD" + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema,
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(mat_executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(mat_executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(
        mat_executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(mat_executor.Execute());

  txn_manager.CommitTransaction(txn);
}

TEST_F(TileGroupLayoutTests, RowLayout) {
  ExecuteTileGroupTest(LayoutType::ROW);
}

TEST_F(TileGroupLayoutTests, ColumnLayout) {
  ExecuteTileGroupTest(LayoutType::COLUMN);
}

}  // namespace test
}  // namespace peloton
