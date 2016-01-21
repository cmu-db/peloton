//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_join_executor.cpp
//
// Identification: src/backend/executor/abstract_join_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/common/exception.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/abstract_join_executor.h"

#include "backend/planner/abstract_join_plan.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

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
  assert(children_.size() == 2);

  // Grab data from plan node.
  const planner::AbstractJoinPlan &node =
      GetPlanNode<planner::AbstractJoinPlan>();

  // NOTE: predicate can be null for cartesian product
  predicate_ = node.GetPredicate();
  proj_info_ = node.GetProjInfo();
  join_type_ = node.GetJoinType();

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
    assert(!proj_info_->isNonTrivial());
    auto &direct_map_list = proj_info_->GetDirectMapList();
    schema.resize(direct_map_list.size());

    LOG_INFO("left size: %lu, right size: %lu", left.size(), right.size());
    LOG_INFO("Projection: %s", proj_info_->Debug().c_str());
    for (auto &entry : direct_map_list) {
      if (entry.second.first == 0) {
        assert(entry.second.second < left.size());
        schema[entry.first] = left[entry.second.second];
      } else {
        assert(entry.second.second < right.size());
        schema[entry.first] = right[entry.second.second];
      }
    }
  }
  return schema;
}

std::unique_ptr<LogicalTile> AbstractJoinExecutor::BuildOutputLogicalTile(
    LogicalTile *left_tile, LogicalTile *right_tile) {
  // Check the input logical tiles.
  assert(left_tile != nullptr);
  assert(right_tile != nullptr);

  // Construct output logical tile.
  std::unique_ptr<LogicalTile> output_tile(LogicalTileFactory::GetTile());

  auto left_tile_schema = left_tile->GetSchema();
  auto right_tile_schema = right_tile->GetSchema();

  for (auto &col : right_tile_schema) {
    col.position_list_idx += left_tile->GetPositionLists().size();
  }

  /* build the schema given the projection */
  auto output_tile_schema = BuildSchema(left_tile_schema, right_tile_schema);

  // Set the output logical tile schema
  output_tile->SetSchema(std::move(output_tile_schema));

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

  assert(left_tile_column_count > 0);
  assert(right_tile_column_count > 0);

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
  assert(join_type_ != JOIN_TYPE_INVALID);
  left_result_tiles_.emplace_back(left_tile);
  switch (join_type_) {
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_OUTER:
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
  assert(join_type_ != JOIN_TYPE_INVALID);
  right_result_tiles_.emplace_back(right_tile);
  switch (join_type_) {
    case JOIN_TYPE_RIGHT:
    case JOIN_TYPE_OUTER:
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
  assert(join_type_ != JOIN_TYPE_INVALID);
  switch (join_type_) {
    case JOIN_TYPE_LEFT:
      UpdateLeftJoinRowSets();
      break;
    case JOIN_TYPE_RIGHT:
      UpdateRightJoinRowSets();
      break;
    case JOIN_TYPE_OUTER:
      UpdateFullJoinRowSets();
      break;
    default:
      break;
  }
}

void AbstractJoinExecutor::UpdateLeftJoinRowSets() {
  assert(left_result_tiles_.size() - no_matching_left_row_sets_.size() == 1);
  no_matching_left_row_sets_.emplace_back(left_result_tiles_.back()->begin(),
                                          left_result_tiles_.back()->end());
}

void AbstractJoinExecutor::UpdateRightJoinRowSets() {
  assert(right_result_tiles_.size() - no_matching_right_row_sets_.size() == 1);
  no_matching_right_row_sets_.emplace_back(right_result_tiles_.back()->begin(),
                                           right_result_tiles_.back()->end());
}

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
  assert(join_type_ != JOIN_TYPE_INVALID);

  switch (join_type_) {
    case JOIN_TYPE_LEFT: {
      return BuildLeftJoinOutput();
    }

    case JOIN_TYPE_RIGHT: {
      return BuildRightJoinOutput();
    }

    case JOIN_TYPE_OUTER: {
      bool status = BuildLeftJoinOutput();

      if (status == true) {
        return status;
      } else {
        return BuildRightJoinOutput();
      }
      break;
    }

    case JOIN_TYPE_INNER: {
      return false;
    }

    default: {
      throw Exception("Unsupported join type : " + std::to_string(join_type_));
      break;
    }
  }

  return false;
}

bool AbstractJoinExecutor::BuildLeftJoinOutput() {
  while (left_matching_idx < no_matching_left_row_sets_.size()) {
    if (no_matching_left_row_sets_[left_matching_idx].empty()) {
      left_matching_idx++;
      continue;
    }

    assert(right_result_tiles_.size() > 0);
    auto left_tile = left_result_tiles_[left_matching_idx].get();
    auto right_tile = right_result_tiles_.front().get();
    auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);

    LogicalTile::PositionListsBuilder pos_lists_builder(left_tile, right_tile);
    for (auto left_row_itr : no_matching_left_row_sets_[left_matching_idx]) {
      pos_lists_builder.AddRightNullRow(left_row_itr);
    }

    assert(pos_lists_builder.Size() > 0);
    output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
    SetOutput(output_tile.release());
    left_matching_idx++;
    return true;
  }
  return false;
}

bool AbstractJoinExecutor::BuildRightJoinOutput() {
  while (right_matching_idx < no_matching_right_row_sets_.size()) {
    if (no_matching_right_row_sets_[right_matching_idx].empty()) {
      right_matching_idx++;
      continue;
    }

    assert(left_result_tiles_.size() > 0);
    auto left_tile = left_result_tiles_.front().get();
    auto right_tile = right_result_tiles_[right_matching_idx].get();
    auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);

    LogicalTile::PositionListsBuilder pos_lists_builder(left_tile, right_tile);
    for (auto right_row_itr : no_matching_right_row_sets_[right_matching_idx]) {
      pos_lists_builder.AddLeftNullRow(right_row_itr);
    }

    assert(pos_lists_builder.Size() > 0);
    output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
    SetOutput(output_tile.release());
    right_matching_idx++;
    return true;
  }
  return false;
}

}  // namespace executor
}  // namespace peloton
