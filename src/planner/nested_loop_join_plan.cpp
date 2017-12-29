//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_plan.cpp
//
// Identification: /peloton/src/planner/nested_loop_join_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "planner/nested_loop_join_plan.h"

#include "common/types.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"

namespace peloton {
namespace planner {

NestedLoopJoinPlan::NestedLoopJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {}

NestedLoopJoinPlan::NestedLoopJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    std::vector<oid_t> &join_column_ids_left,
    std::vector<oid_t> &join_column_ids_right)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {
  join_column_ids_left_ = join_column_ids_left;
  join_column_ids_right_ = join_column_ids_right;
}

}  // namespace planner
}  // namespace peloton
