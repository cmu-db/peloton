//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_plan.cpp
//
// Identification: src/planner/hash_join_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "type/types.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"

#include "planner/hash_join_plan.h"

namespace peloton {
namespace planner {

HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {}

HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    const std::vector<oid_t>
        &outer_hashkeys)  // outer_hashkeys is added for IN-subquery
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {
  outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
}

void HashJoinPlan::HandleSubplanBinding(
    bool is_left, const BindingContext &input) {
  auto &keys = is_left ? left_hash_keys_ : right_hash_keys_;
  for (auto &key : keys) {
    auto *key_exp = const_cast<expression::AbstractExpression *>(key.get());
    key_exp->PerformBinding({&input});
  }
}

}  // namespace planner
}  // namespace peloton
