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
#include <vector>

#include "planner/nested_loop_join_plan.h"

namespace peloton {
namespace planner {

NestedLoopJoinPlan::NestedLoopJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    const std::vector<oid_t> &join_column_ids_left,
    const std::vector<oid_t> &join_column_ids_right)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {
  join_column_ids_left_ = join_column_ids_left;
  join_column_ids_right_ = join_column_ids_right;
}

void NestedLoopJoinPlan::HandleSubplanBinding(bool from_left,
                                              const BindingContext &ctx) {
  if (from_left) {
    for (const auto left_col_id : join_column_ids_left_) {
      const auto *ai = ctx.Find(left_col_id);
      PL_ASSERT(ai != nullptr);
      join_ais_left_.push_back(ai);
    }
  } else {
    for (const auto right_col_id : join_column_ids_right_) {
      const auto *ai = ctx.Find(right_col_id);
      PL_ASSERT(ai != nullptr);
      join_ais_right_.push_back(ai);
    }
  }
}

std::unique_ptr<AbstractPlan> NestedLoopJoinPlan::Copy() const {
  std::unique_ptr<const expression::AbstractExpression> predicate_copy(
      GetPredicate()->Copy());

  std::shared_ptr<const catalog::Schema> schema_copy(
      catalog::Schema::CopySchema(GetSchema()));

  NestedLoopJoinPlan *new_plan = new NestedLoopJoinPlan(
      GetJoinType(), std::move(predicate_copy), GetProjInfo()->Copy(),
      schema_copy, join_column_ids_left_, join_column_ids_right_);

  return std::unique_ptr<AbstractPlan>(new_plan);
}

hash_t NestedLoopJoinPlan::Hash() const {
  hash_t hash = AbstractJoinPlan::Hash();

  // In addition to everything, hash the left and right join columns
  for (const auto &left_col_id : GetJoinColumnsLeft()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&left_col_id));
  }
  for (const auto &right_col_id : GetJoinColumnsRight()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&right_col_id));
  }

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool NestedLoopJoinPlan::operator==(const AbstractPlan &rhs) const {
  if (!AbstractJoinPlan::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const NestedLoopJoinPlan &>(rhs);

  return GetJoinColumnsLeft() == other.GetJoinColumnsLeft() &&
         GetJoinColumnsRight() == other.GetJoinColumnsRight() &&
         AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
