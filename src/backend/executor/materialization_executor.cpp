//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// materialization_executor.cpp
//
// Identification: src/backend/executor/materialization_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/materialization_executor.h"

#include <cassert>
#include <memory>
#include <utility>

#include "backend/common/logger.h"
#include "backend/planner/materialization_plan.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"

namespace peloton {
namespace executor {

// Row-oriented materialization
void MaterializeRowAtAtATime(LogicalTile *source_tile,
                             const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
                             const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
                             storage::Tile *dest_tile);

// Column-oriented materialization
void MaterializeColumnAtATime(LogicalTile *source_tile,
                              const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
                              const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
                              storage::Tile *dest_tile);

/**
 * @brief Constructor for the materialization executor.
 * @param node Materialization node corresponding to this executor.
 */
MaterializationExecutor::MaterializationExecutor(
    const planner::AbstractPlan *node, ExecutorContext *executor_context)
: AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool MaterializationExecutor::DInit() {
  assert(children_.size() == 1);

  return true;
}

/**
 * @brief Generates map from each base tile to columns originally from that
 *        base tile to be materialized.
 * @param column_ids Ids of columns to be materialized.
 * @param source_tile Logical tile that contains mapping from columns to
 *        base tiles.
 * @param tile_to_cols Map to be populated with mappings from tile to columns.
 *
 * We generate this mapping so that we can materialize columns tile by tile for
 * efficiency reasons.
 */
void MaterializationExecutor::GenerateTileToColMap(
    const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
    LogicalTile *source_tile,
    std::unordered_map<storage::Tile *, std::vector<oid_t>> &
    cols_in_physical_tile) {
  for (const auto &kv : old_to_new_cols) {
    oid_t col = kv.first;

    // figure out base physical tile for column in logical tile
    storage::Tile *base_tile = source_tile->GetBaseTile(col);

    std::vector<oid_t> &cols_from_tile = cols_in_physical_tile[base_tile];
    cols_from_tile.push_back(col);
  }
}

/**
 * @brief Does the actual copying of data into the new physical tile.
 * @param source_tile Source tile to copy data from.
 * @param tile_to_cols Map from base tile to columns in that tile
 *        to be materialized.
 * @param dest_tile New tile to copy data into.
 */
void MaterializationExecutor::MaterializeByTiles(
    LogicalTile *source_tile,
    const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
    const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
    storage::Tile *dest_tile) {

  auto dest_tile_column_count = dest_tile->GetColumnCount();
  // TODO: Make this a parameter
  oid_t column_count_threshold = 20;
  bool row_wise_materialization = true;

  if(peloton_layout == LAYOUT_COLUMN)
      row_wise_materialization = false;

  if(peloton_layout == LAYOUT_HYBRID &&
      dest_tile_column_count < column_count_threshold)
    row_wise_materialization = false;

  // Materialize as needed
  if(row_wise_materialization == true) {
    MaterializeRowAtAtATime(source_tile, old_to_new_cols, tile_to_cols, dest_tile);
  }
  else {
    MaterializeColumnAtATime(source_tile, old_to_new_cols, tile_to_cols, dest_tile);
  }

}

void MaterializeRowAtAtATime(LogicalTile *source_tile,
                             const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
                             const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
                             storage::Tile *dest_tile) {

  ///////////////////////////
  // EACH PHYSICAL TILE
  ///////////////////////////
  // Copy over all data from each base tile.
  for (const auto &kv : tile_to_cols) {
    const std::vector<oid_t> &old_column_ids = kv.second;

    auto &schema = source_tile->GetSchema();
    oid_t new_tuple_id = 0;

    auto& column_position_lists = source_tile->GetPositionLists();

    // Get old column information
    std::vector<oid_t> old_column_position_idxs;
    std::vector<size_t> old_column_offsets;
    std::vector<ValueType> old_column_types;
    std::vector<bool> old_is_inlineds;
    std::vector<storage::Tile*> old_tiles;

    // Get new column information
    std::vector<size_t> new_column_offsets;
    std::vector<bool> new_is_inlineds;
    std::vector<size_t> new_column_lengths;

    // Amortize schema lookups once per column
    for (oid_t old_col_id : old_column_ids) {
      auto& column_info = schema[old_col_id];

      // Get the position list
      old_column_position_idxs.push_back(column_info.position_list_idx);

      // Get old column information
      storage::Tile *old_tile = column_info.base_tile_ref.get();
      old_tiles.push_back(old_tile);
      auto old_schema = old_tile->GetSchema();
      oid_t old_column_id = column_info.origin_column_id;
      const size_t old_column_offset = old_schema->GetOffset(old_column_id);
      old_column_offsets.push_back(old_column_offset);
      const ValueType old_column_type = old_schema->GetType(old_column_id);
      old_column_types.push_back(old_column_type);
      const bool old_is_inlined = old_schema->IsInlined(old_column_id);
      old_is_inlineds.push_back(old_is_inlined);

      // Old to new column mapping
      auto it = old_to_new_cols.find(old_col_id);
      assert(it != old_to_new_cols.end());

      // Get new column information
      oid_t new_column_id = it->second;
      auto new_schema = dest_tile->GetSchema();
      const size_t new_column_offset = new_schema->GetOffset(new_column_id);
      new_column_offsets.push_back(new_column_offset);
      const bool new_is_inlined = new_schema->IsInlined(new_column_id);
      new_is_inlineds.push_back(new_is_inlined);
      const size_t new_column_length = new_schema->GetAppropriateLength(new_column_id);
      new_column_lengths.push_back(new_column_length);
    }

    assert(new_column_offsets.size() == old_column_ids.size());

    ///////////////////////////
    // EACH TUPLE
    ///////////////////////////
    // Copy all values in the tuple to the physical tile
    // This uses fast getter and setter functions
    for (oid_t old_tuple_id : *source_tile) {

      ///////////////////////////
      // EACH COLUMN
      ///////////////////////////
      // Go over each column in given base physical tile
      oid_t col_itr = 0;

      for (oid_t old_col_id : old_column_position_idxs) {

        auto& column_position_list = column_position_lists[old_col_id];

        oid_t base_tuple_id = column_position_list[old_tuple_id];

        auto value = old_tiles[col_itr]->GetValueFast(base_tuple_id,
                                                      old_column_offsets[col_itr],
                                                      old_column_types[col_itr],
                                                      old_is_inlineds[col_itr]);

        LOG_TRACE("Old Tuple : %u Column : %u \n", old_tuple_id, old_col_id);
        LOG_TRACE("New Tuple : %u Column : %u \n", new_tuple_id, new_column_id);

        dest_tile->SetValueFast(value, new_tuple_id,
                                new_column_offsets[col_itr],
                                new_is_inlineds[col_itr],
                                new_column_lengths[col_itr]);

        // Go to next column
        col_itr++;
      }

      // Go to next tuple
      new_tuple_id++;
    }

  }

}

void MaterializeColumnAtATime(LogicalTile *source_tile,
                              const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
                              const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
                              storage::Tile *dest_tile) {

  ///////////////////////////
  // EACH PHYSICAL TILE
  ///////////////////////////
  // Copy over all data from each base tile.
  for (const auto &kv : tile_to_cols) {
    const std::vector<oid_t> &old_column_ids = kv.second;

    ///////////////////////////
    // EACH COLUMN
    ///////////////////////////
    // Go over each column in given base physical tile
    for (oid_t old_col_id : old_column_ids) {
      auto &column_info = source_tile->GetColumnInfo(old_col_id);

      // Amortize schema lookups once per column
      storage::Tile *old_tile = column_info.base_tile_ref.get();
      auto old_schema = old_tile->GetSchema();

      // Get old column information
      oid_t old_column_id = column_info.origin_column_id;
      const size_t old_column_offset = old_schema->GetOffset(old_column_id);
      const ValueType old_column_type = old_schema->GetType(old_column_id);
      const bool old_is_inlined = old_schema->IsInlined(old_column_id);

      // Old to new column mapping
      auto it = old_to_new_cols.find(old_col_id);
      assert(it != old_to_new_cols.end());

      // Get new column information
      oid_t new_column_id = it->second;
      auto new_schema = dest_tile->GetSchema();
      const size_t new_column_offset = new_schema->GetOffset(new_column_id);
      const bool new_is_inlined = new_schema->IsInlined(new_column_id);
      const size_t new_column_length = new_schema->GetAppropriateLength(new_column_id);

      // Get the position list
      auto& column_position_list = source_tile->GetPositionList(column_info.position_list_idx);
      oid_t new_tuple_id = 0;

      // Copy all values in the column to the physical tile
      // This uses fast getter and setter functions
      ///////////////////////////
      // EACH TUPLE
      ///////////////////////////
      for (oid_t old_tuple_id : *source_tile) {
        oid_t base_tuple_id = column_position_list[old_tuple_id];
        auto value = old_tile->GetValueFast(base_tuple_id,
                                            old_column_offset,
                                            old_column_type,
                                            old_is_inlined);

        LOG_TRACE("Old Tuple : %u Column : %u \n", old_tuple_id, old_col_id);
        LOG_TRACE("New Tuple : %u Column : %u \n", new_tuple_id, new_column_id);

        dest_tile->SetValueFast(value, new_tuple_id,
                                new_column_offset,
                                new_is_inlined,
                                new_column_length);

        // Go to next tuple
        new_tuple_id++;
      }

    }

  }

}

std::unordered_map<oid_t, oid_t> MaterializationExecutor::BuildIdentityMapping(
    const catalog::Schema *schema) {
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t column_count = schema->GetColumnCount();
  for (oid_t col = 0; col < column_count; col++) {
    old_to_new_cols[col] = col;
  }

  return old_to_new_cols;
}

/**
 * @brief Create a physical tile for the given logical tile
 * @param source_tile Source tile from which the physical tile is created
 * @return a logical tile wrapper for the created physical tile
 */
LogicalTile *MaterializationExecutor::Physify(LogicalTile *source_tile) {
  std::unique_ptr<catalog::Schema> source_tile_schema(
      source_tile->GetPhysicalSchema());
  const int num_tuples = source_tile->GetTupleCount();
  const catalog::Schema *output_schema;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  auto node = GetRawNode();

  // Create a default identity mapping node if we did not get one
  if (node == nullptr) {
    assert(source_tile_schema.get());
    output_schema = source_tile_schema.get();

    old_to_new_cols = BuildIdentityMapping(output_schema);
  }
  // Else use the mapping in the given plan node
  else {
    const planner::MaterializationPlan &node =
        GetPlanNode<planner::MaterializationPlan>();
    if (node.GetSchema()) {
      output_schema = node.GetSchema();
      old_to_new_cols = node.old_to_new_cols();
    } else {
      output_schema = source_tile_schema.get();
      old_to_new_cols = BuildIdentityMapping(output_schema);
    }
  }

  // Generate mappings.
  std::unordered_map<storage::Tile *, std::vector<oid_t>> tile_to_cols;
  GenerateTileToColMap(old_to_new_cols, source_tile, tile_to_cols);

  // Create new physical tile.
  std::shared_ptr<storage::Tile> dest_tile(
      storage::TileFactory::GetTempTile(*output_schema, num_tuples));

  // Proceed to materialize logical tile by physical tile at a time.
  MaterializeByTiles(source_tile, old_to_new_cols, tile_to_cols,
                     dest_tile.get());

  // Wrap physical tile in logical tile.
  return LogicalTileFactory::WrapTiles({dest_tile});
}

/**
 * @brief Creates materialized physical tile from logical tile and wraps it
 *        in a new logical tile.
 *
 * @return true on success, false otherwise.
 */
bool MaterializationExecutor::DExecute() {
  // Retrieve child tile.
  const bool success = children_[0]->Execute();
  if (!success) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());
  LogicalTile *output_tile = nullptr;

  // Check the number of tuples in input logical tile
  // If none, then just return false
  const int num_tuples = source_tile->GetTupleCount();
  if (num_tuples == 0) {
    return false;
  }

  auto node = GetRawNode();
  bool physify_flag = true;  // by default, we create a physical tile

  if (node != nullptr) {
    const planner::MaterializationPlan &node =
        GetPlanNode<planner::MaterializationPlan>();
    physify_flag = node.GetPhysifyFlag();
  }

  if (physify_flag) {
    /* create a physical tile and a logical tile wrapper to be the output */
    output_tile = Physify(source_tile.get());
  } else {
    /* just pass thru the underlying logical tile */
    output_tile = source_tile.release();
  }

  SetOutput(output_tile);

  return true;
}

}  // namespace executor
}  // namespace peloton
