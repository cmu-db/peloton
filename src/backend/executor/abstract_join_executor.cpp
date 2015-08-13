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

#include "../planner/abstract_join_plan.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for nested loop join executor.
 * @param node Nested loop join node corresponding to this executor.
 */
AbstractJoinExecutor::AbstractJoinExecutor(planner::AbstractPlan *node,
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

  return true;
}

/**
 * @ brief Build the schema of the joined tile based on the projection info
 */
std::vector<LogicalTile::ColumnInfo> AbstractJoinExecutor::BuildSchema(
    std::vector<LogicalTile::ColumnInfo> left,
    std::vector<LogicalTile::ColumnInfo> right) {
  std::vector<LogicalTile::ColumnInfo> schema;
  if (proj_info_ == nullptr) {
    // no projection
    schema.assign(left.begin(), left.end());
    schema.insert(schema.end(), right.begin(), right.end());
  } else {
    assert(!proj_info_->isNonTrivial());
    auto &direct_map_list = proj_info_->GetDirectMapList();
    schema.resize(direct_map_list.size());
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

}  // namespace executor
}  // namespace peloton
