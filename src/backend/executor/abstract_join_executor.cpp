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
    LogicalTile *left_tile,
    LogicalTile *right_tile) {
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

std::vector<std::vector<oid_t> > AbstractJoinExecutor::BuildPostitionLists(
    LogicalTile *left_tile,
    LogicalTile *right_tile) {
  // Get position list from two logical tiles
  auto &left_tile_position_lists = left_tile->GetPositionLists();
  auto &right_tile_position_lists = right_tile->GetPositionLists();

  // Compute the output logical tile column count
  size_t left_tile_column_count = left_tile_position_lists.size();
  size_t right_tile_column_count = right_tile_position_lists.size();
  size_t output_tile_column_count = left_tile_column_count
      + right_tile_column_count;

  assert(left_tile_column_count > 0);
  assert(right_tile_column_count > 0);

  // Construct position lists for output tile
  std::vector<std::vector<oid_t> > position_lists;
  for (size_t column_itr = 0; column_itr < output_tile_column_count; column_itr++)
    position_lists.push_back(std::vector<oid_t>());

  return position_lists;
}



}  // namespace executor
}  // namespace peloton
