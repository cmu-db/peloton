//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats.cpp
//
// Identification: src/optimizer/stats/table_stats.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/table_stats.h"

#include <memory>

#include "common/macros.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace optimizer {

TableStats::TableStats(storage::DataTable* table)
    : table_(table),
      column_stats_{},
      active_tuple_count_{0},
      column_count_{0} {}

TableStats::~TableStats() {}

void TableStats::CollectColumnStats() {
  schema_ = table_->GetSchema();
  column_count_ = schema_->GetColumnCount();

  // Ignore empty table
  if (column_count_ == 0) {
    return;
  }

  InitColumnStats();

  size_t tile_group_count = table_->GetTileGroupCount();
  // Collect stats for all tile groups.
  for (size_t offset = 0; offset < tile_group_count; offset++) {
    std::shared_ptr<storage::TileGroup> tile_group =
        table_->GetTileGroup(offset);
    storage::TileGroupHeader* tile_group_header = tile_group->GetHeader();
    oid_t tuple_count = tile_group->GetAllocatedTupleCount();
    active_tuple_count_ += tile_group_header->GetActiveTupleCount();
    // Collect stats for all tuples in the tile group.
    for (oid_t tuple_id = 0; tuple_id < tuple_count; tuple_id++) {
      txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
      if (tuple_txn_id != INVALID_TXN_ID) {
        // Collect stats for all columns.
        for (oid_t column_id = 0; column_id < column_count_; column_id++) {
          type::Value value = tile_group->GetValue(tuple_id, column_id);
          column_stats_[column_id]->AddValue(value);
        } /* column */
      }
    } /* tuple */
  }   /* tile group */
}

void TableStats::InitColumnStats() {
  oid_t database_id = table_->GetDatabaseOid();
  oid_t table_id = table_->GetOid();
  for (oid_t column_id = 0; column_id < column_count_; column_id++) {
    std::unique_ptr<ColumnStats> colstats(new ColumnStats(
        database_id, table_id, column_id, schema_->GetType(column_id)));
    column_stats_.push_back(std::move(colstats));
  }
}

ColumnStats* TableStats::GetColumnStats(oid_t column_id) {
  PL_ASSERT(column_id < column_stats_.size());
  return column_stats_[column_id].get();
}

}  // namespace optimizer
}  // namespace peloton
