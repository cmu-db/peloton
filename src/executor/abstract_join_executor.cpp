//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_join_executor.cpp
//
// Identification: src/executor/abstract_join_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "common/internal_types.h"
#include "common/logger.h"
#include "common/exception.h"
#include "executor/logical_tile_factory.h"
#include "executor/abstract_join_executor.h"

#include "planner/abstract_join_plan.h"
#include "expression/abstract_expression.h"
#include "common/container_tuple.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for nested loop join executor.
 * @param node Nested loop join node corresponding to this executor.
 */
AbstractJoinExecutor::AbstractJoinExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and create the schema for the output logical
 * tiles.
 * @return true on success, false otherwise.
 */
bool AbstractJoinExecutor::DInit() {
  PL_ASSERT(children_.size() == 2);

  // Grab data from plan node.
  const planner::AbstractJoinPlan &node =
      GetPlanNode<planner::AbstractJoinPlan>();

  // NOTE: predicate can be null for cartesian product
  predicate_ = node.GetPredicate();
  proj_info_ = node.GetProjInfo();
  join_type_ = node.GetJoinType();
  proj_schema_ = node.GetSchema();

  return true;
}

/**
 * @ brief Build the schema of the joined tile based on the projection info
 */
std::vector<LogicalTile::ColumnInfo> AbstractJoinExecutor::BuildSchema(
    std::vector<LogicalTile::ColumnInfo> &left,
    std::vector<LogicalTile::ColumnInfo> &right) {
  std::vector<LogicalTile::ColumnInfo> schema;
  if (proj_info_ == nullptr) {
    // no projection
    schema.assign(left.begin(), left.end());
    schema.insert(schema.end(), right.begin(), right.end());
  } else {
    PL_ASSERT(!proj_info_->IsNonTrivial());
    auto &direct_map_list = proj_info_->GetDirectMapList();
    schema.resize(direct_map_list.size());

    LOG_TRACE("left size: %lu, right size: %lu", left.size(), right.size());
    LOG_TRACE("Projection: %s", proj_info_->Debug().c_str());
    for (auto &entry : direct_map_list) {
      if (entry.second.first == 0) {
        PL_ASSERT(entry.second.second < left.size());
        schema[entry.first] = left[entry.second.second];
      } else {
        PL_ASSERT(entry.second.second < right.size());
        schema[entry.first] = right[entry.second.second];
      }
    }
  }
  return schema;
}
std::vector<LogicalTile::ColumnInfo>
AbstractJoinExecutor::BuildSchemaFromLeftTile(
    const std::vector<LogicalTile::ColumnInfo> *left_schema,
    const catalog::Schema *output_schema, oid_t left_pos_list_count) {
  PL_ASSERT(left_schema != nullptr);

  // dummy physical tile for the empty child
  std::shared_ptr<storage::Tile> ptile(storage::TileFactory::GetTile(
      BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      nullptr, *output_schema, nullptr, 1));

  std::vector<LogicalTile::ColumnInfo> schema;
  auto total_size = output_schema->GetColumnCount();
  if (proj_info_ == nullptr) {
    // no projection. each column's of the right tile maps to the last position
    // list
    schema.insert(schema.end(), left_schema->begin(), left_schema->end());
    for (oid_t right_col_idx = 0;
         right_col_idx < total_size - left_schema->size(); right_col_idx++) {
      LogicalTile::ColumnInfo col;
      col.base_tile = ptile;
      col.origin_column_id = right_col_idx;
      col.position_list_idx = left_pos_list_count;
      schema.push_back(col);
    }
  } else {
    // non trivial projection. construct from direct map list
    PL_ASSERT(!proj_info_->IsNonTrivial());
    auto &direct_map_list = proj_info_->GetDirectMapList();
    schema.resize(direct_map_list.size());

    for (auto &entry : direct_map_list) {
      auto schema_col_idx = entry.first;
      // map right column to output tile column
      if (entry.second.first == 1) {
        LogicalTile::ColumnInfo col;
        col.base_tile = ptile;
        col.origin_column_id = schema_col_idx;
        col.position_list_idx = left_pos_list_count;
        schema[schema_col_idx] = col;
      } else {
        // map left column to output tile column
        PL_ASSERT(entry.second.second < left_schema->size());
        schema[schema_col_idx] = (*left_schema)[entry.second.second];
      }
    }
  }
  PL_ASSERT(schema.size() == total_size);
  return schema;
}

std::vector<LogicalTile::ColumnInfo>
AbstractJoinExecutor::BuildSchemaFromRightTile(
    const std::vector<LogicalTile::ColumnInfo> *right_schema,
    const catalog::Schema *output_schema) {
  PL_ASSERT(right_schema != nullptr);
  // dummy physical tile for the empty child tile
  std::shared_ptr<storage::Tile> ptile(storage::TileFactory::GetTile(
      BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
      nullptr, *output_schema, nullptr, 1));

  std::vector<LogicalTile::ColumnInfo> schema;
  auto total_size = output_schema->GetColumnCount();
  if (proj_info_ == nullptr) {
    // no projection. each column's of the right tile maps to the first position
    // list
    for (oid_t left_col_idx = 0;
         left_col_idx < total_size - right_schema->size(); left_col_idx++) {
      LogicalTile::ColumnInfo col;
      col.base_tile = ptile;
      col.origin_column_id = left_col_idx;
      col.position_list_idx = 0;
      schema.push_back(col);
    }
    schema.insert(schema.end(), right_schema->begin(), right_schema->end());
  } else {
    // non trivial projection. construct from direct map list
    PL_ASSERT(!proj_info_->IsNonTrivial());
    auto &direct_map_list = proj_info_->GetDirectMapList();
    schema.resize(direct_map_list.size());

    for (auto &entry : direct_map_list) {
      auto schema_col_idx = entry.first;
      // map left column to output tile column
      if (entry.second.first == 0) {
        LogicalTile::ColumnInfo col;
        col.base_tile = ptile;
        col.origin_column_id = schema_col_idx;
        col.position_list_idx = 0;
        schema[schema_col_idx] = col;
      } else {
        // map right column to output tile column
        PL_ASSERT(entry.second.second < right_schema->size());
        schema[schema_col_idx] = (*right_schema)[entry.second.second];
        // reserve the left-most position list for left tile
        schema[schema_col_idx].position_list_idx += 1;
      }
    }
  }
  PL_ASSERT(schema.size() == total_size);
  return schema;
}

/**
 * @ brief Build the joined tile with schema derived from children tiles
 */
std::unique_ptr<LogicalTile> AbstractJoinExecutor::BuildOutputLogicalTile(
    LogicalTile *left_tile, LogicalTile *right_tile) {
  // Check the input logical tiles.
  PL_ASSERT(left_tile != nullptr);
  PL_ASSERT(right_tile != nullptr);

  // Construct output logical tile.
  std::unique_ptr<LogicalTile> output_tile(LogicalTileFactory::GetTile());

  auto left_tile_schema = left_tile->GetSchema();
  auto right_tile_schema = right_tile->GetSchema();

  // advance the position list index of right tile schema
  for (auto &col : right_tile_schema) {
    col.position_list_idx += left_tile->GetPositionLists().size();
  }

  /* build the schema given the projection */
  auto output_tile_schema = BuildSchema(left_tile_schema, right_tile_schema);

  // Set the output logical tile schema
  output_tile->SetSchema(std::move(output_tile_schema));

  return output_tile;
}

std::unique_ptr<LogicalTile> AbstractJoinExecutor::BuildOutputLogicalTile(
    LogicalTile *left_tile, LogicalTile *right_tile,
    const catalog::Schema *output_schema) {
  PL_ASSERT(output_schema != nullptr);

  std::unique_ptr<LogicalTile> output_tile(LogicalTileFactory::GetTile());

  // get the non empty tile
  LogicalTile *non_empty_tile = GetNonEmptyTile(left_tile, right_tile);

  auto non_empty_tile_schema = non_empty_tile->GetSchema();

  // left tile is empty
  if (non_empty_tile == right_tile) {
    /* build the schema given the projection */
    output_tile->SetSchema(
        BuildSchemaFromRightTile(&non_empty_tile_schema, output_schema));
  } else {
    // right tile is empty
    output_tile->SetSchema(
        BuildSchemaFromLeftTile(&non_empty_tile_schema, output_schema,
                                left_tile->GetPositionLists().size()));
  }
  return output_tile;
}

std::vector<std::vector<oid_t>> AbstractJoinExecutor::BuildPostitionLists(
    LogicalTile *left_tile, LogicalTile *right_tile) {
  // Get position list from two logical tiles
  auto &left_tile_position_lists = left_tile->GetPositionLists();
  auto &right_tile_position_lists = right_tile->GetPositionLists();

  // Compute the output logical tile column count
  size_t left_tile_column_count = left_tile_position_lists.size();
  size_t right_tile_column_count = right_tile_position_lists.size();
  size_t output_tile_column_count =
      left_tile_column_count + right_tile_column_count;

  PL_ASSERT(left_tile_column_count > 0);
  PL_ASSERT(right_tile_column_count > 0);

  // Construct position lists for output tile
  std::vector<std::vector<oid_t>> position_lists;
  for (size_t column_itr = 0; column_itr < output_tile_column_count;
       column_itr++)
    position_lists.push_back(std::vector<oid_t>());

  return position_lists;
}

/**
 * Buffer logical tiles got from executing child executors
 * This will also initialize a new join row set that belongs
 * to the new result tile
 */
void AbstractJoinExecutor::BufferLeftTile(LogicalTile *left_tile) {
  PL_ASSERT(join_type_ != JoinType::INVALID);
  left_result_tiles_.emplace_back(left_tile);
  switch (join_type_) {
    case JoinType::LEFT:
    case JoinType::OUTER:
      UpdateLeftJoinRowSets();
      break;
    default:
      break;
  }
}

/**
 * Buffer logical tiles got from executing child executors
 * This will also initialize a new join row set that belongs
 * to the new result tile
 */
void AbstractJoinExecutor::BufferRightTile(LogicalTile *right_tile) {
  PL_ASSERT(join_type_ != JoinType::INVALID);
  right_result_tiles_.emplace_back(right_tile);
  switch (join_type_) {
    case JoinType::RIGHT:
    case JoinType::OUTER:
      UpdateRightJoinRowSets();
      break;
    default:
      break;
  }
}

/**
 * Update join row sets depending on types of join.
 * When new result tile is buffered, matching status of the
 * rows in the new tile should be tracked.
 * This is called by BufferTile routines, as join row sets
 * should be updated when new result tile is buffered
 */
void AbstractJoinExecutor::UpdateJoinRowSets() {
  PL_ASSERT(join_type_ != JoinType::INVALID);
  switch (join_type_) {
    case JoinType::LEFT:
      UpdateLeftJoinRowSets();
      break;
    case JoinType::RIGHT:
      UpdateRightJoinRowSets();
      break;
    case JoinType::OUTER:
      UpdateFullJoinRowSets();
      break;
    default:
      break;
  }
}

/**
  * Update the row set with all rows from the last tile from left child
  */
void AbstractJoinExecutor::UpdateLeftJoinRowSets() {
  PL_ASSERT(left_result_tiles_.size() - no_matching_left_row_sets_.size() == 1);
  no_matching_left_row_sets_.emplace_back(left_result_tiles_.back()->begin(),
                                          left_result_tiles_.back()->end());
}

/**
  * Update the row set with all rows from the last tile from right child
  */
void AbstractJoinExecutor::UpdateRightJoinRowSets() {
  PL_ASSERT(right_result_tiles_.size() - no_matching_right_row_sets_.size() ==
            1);
  no_matching_right_row_sets_.emplace_back(right_result_tiles_.back()->begin(),
                                           right_result_tiles_.back()->end());
}

/**
  * Update the row set with all rows from the last tile from both child
  */
void AbstractJoinExecutor::UpdateFullJoinRowSets() {
  UpdateLeftJoinRowSets();
  UpdateRightJoinRowSets();
}

/**
 * In some case, outer join results can be determined only after all inner
 * join results are constructed, because, in order to build outer join result,
 * we need to know the rows from one side that cannot be matched by rows from
 * the other side. If inner join part has not finished, we cannot know whether
 * there will be a match later.
 */
bool AbstractJoinExecutor::BuildOuterJoinOutput() {
  PL_ASSERT(join_type_ != JoinType::INVALID);

  switch (join_type_) {
    case JoinType::LEFT: { return BuildLeftJoinOutput(); }

    case JoinType::RIGHT: { return BuildRightJoinOutput(); }

    case JoinType::OUTER: {
      bool status = BuildLeftJoinOutput();

      if (status == true) {
        return status;
      } else {
        return BuildRightJoinOutput();
      }
      break;
    }

    case JoinType::INNER: { return false; }

    default: {
      throw Exception("Unsupported join type : " + JoinTypeToString(join_type_));
      break;
    }
  }

  return false;
}
/*
  * build left join output by adding null rows for every row from right tile
  * which doesn't have a match
  */

bool AbstractJoinExecutor::BuildLeftJoinOutput() {
  while (left_matching_idx < no_matching_left_row_sets_.size()) {
    if (no_matching_left_row_sets_[left_matching_idx].empty()) {
      left_matching_idx++;
      continue;
    }

    std::unique_ptr<LogicalTile> output_tile(nullptr);
    auto left_tile = left_result_tiles_[left_matching_idx].get();
    LogicalTile::PositionListsBuilder pos_lists_builder;
    if (right_result_tiles_.size() == 0) {
      // no tile information for right tile. construct a output tile from left
      // tile only
      output_tile = BuildOutputLogicalTile(left_tile, nullptr, proj_schema_);
      pos_lists_builder = LogicalTile::PositionListsBuilder(
          &(left_tile->GetPositionLists()), nullptr);
    } else {
      PL_ASSERT(right_result_tiles_.size() > 0);
      // construct the output tile from both children tiles
      auto right_tile = right_result_tiles_.front().get();
      output_tile = BuildOutputLogicalTile(left_tile, right_tile);
      pos_lists_builder =
          LogicalTile::PositionListsBuilder(left_tile, right_tile);
    }
    // add rows with null values on the left
    for (auto left_row_itr : no_matching_left_row_sets_[left_matching_idx]) {
      pos_lists_builder.AddRightNullRow(left_row_itr);
    }

    PL_ASSERT(pos_lists_builder.Size() > 0);
    output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
    SetOutput(output_tile.release());
    left_matching_idx++;
    return true;
  }
  return false;
}

/*
 * build right join output by adding null rows for every row from left tile
 * which doesn't have a match
 */
bool AbstractJoinExecutor::BuildRightJoinOutput() {
  while (right_matching_idx < no_matching_right_row_sets_.size()) {
    if (no_matching_right_row_sets_[right_matching_idx].empty()) {
      right_matching_idx++;
      continue;
    }

    std::unique_ptr<LogicalTile> output_tile(nullptr);
    auto right_tile = right_result_tiles_[right_matching_idx].get();
    LogicalTile::PositionListsBuilder pos_lists_builder;
    if (left_result_tiles_.size() == 0) {
      // no tile information for left tile. construct a output tile from right
      // tile only
      output_tile = BuildOutputLogicalTile(nullptr, right_tile, proj_schema_);
      pos_lists_builder = LogicalTile::PositionListsBuilder(
          nullptr, &(right_tile->GetPositionLists()));
    } else {
      PL_ASSERT(left_result_tiles_.size() > 0);
      // construct the output tile from both children tiles
      auto left_tile = left_result_tiles_.front().get();
      output_tile = BuildOutputLogicalTile(left_tile, right_tile);
      pos_lists_builder =
          LogicalTile::PositionListsBuilder(left_tile, right_tile);
    }
    // add rows with null values on the left
    for (auto right_row_itr : no_matching_right_row_sets_[right_matching_idx]) {
      pos_lists_builder.AddLeftNullRow(right_row_itr);
    }
    PL_ASSERT(pos_lists_builder.Size() > 0);
    output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());

    SetOutput(output_tile.release());
    right_matching_idx++;
    return true;
  }
  return false;
}

}  // namespace executor
}  // namespace peloton
