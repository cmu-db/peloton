//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.cpp
//
// Identification: src/planner/abstract_join_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/abstract_join_plan.h"

#include <numeric>

namespace peloton {
namespace planner {

void AbstractJoinPlan::GetOutputColumns(std::vector<oid_t> &columns) const {
  columns.resize(GetSchema()->GetColumnCount());
  std::iota(columns.begin(), columns.end(), 0);
}

void AbstractJoinPlan::PerformBinding(BindingContext &context) {
  const auto &children = GetChildren();
  PL_ASSERT(children.size() == 2);

  // Let the left and right child populate bind their attributes
  BindingContext left_context, right_context;
  children[0]->PerformBinding(left_context);
  children[1]->PerformBinding(right_context);

  HandleSubplanBinding(/*is_left*/ true, left_context);
  HandleSubplanBinding(/*is_left*/ false, right_context);

  // Handle the projection
  std::vector<const BindingContext *> inputs = {&left_context, &right_context};
  GetProjInfo()->PerformRebinding(context, inputs);

  // Now we pull out all the attributes coming from each of the joins children
  std::vector<std::vector<oid_t>> input_cols = {{}, {}};
  GetProjInfo()->PartitionInputs(input_cols);

  // Pull out the left and right non-key attributes
  const auto &left_inputs = input_cols[0];
  for (oid_t left_input_col : left_inputs) {
    left_attributes_.push_back(left_context.Find(left_input_col));
  }
  const auto &right_inputs = input_cols[1];
  for (oid_t right_input_col : right_inputs) {
    right_attributes_.push_back(right_context.Find(right_input_col));
  }

  // The predicate (if one exists) operates on the __output__ of the join and,
  // therefore, will bind against the resulting binding context
  const auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    const_cast<expression::AbstractExpression *>(predicate)
        ->PerformBinding({&left_context, &right_context});
  }
}

void AbstractJoinPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  auto *predicate =
      const_cast<expression::AbstractExpression *>(GetPredicate());
  if (predicate != nullptr) {
    predicate->VisitParameters(map, values, values_from_user);
  }

  auto *projection = const_cast<planner::ProjectInfo *>(GetProjInfo());
  if (projection != nullptr && projection->IsNonTrivial()) {
    for (const auto &target : projection->GetTargetList()) {
      auto *derived_attr_expr =
          const_cast<expression::AbstractExpression *>(target.second.expr);
      derived_attr_expr->VisitParameters(map, values, values_from_user);
    }
  }
}

bool AbstractJoinPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  // Check join type
  auto &other = static_cast<const planner::AbstractJoinPlan &>(rhs);
  if (GetJoinType() != other.GetJoinType()) {
    return false;
  }

  // Check predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  // Check projection information
  auto *proj_info = GetProjInfo();
  auto *other_proj_info = other.GetProjInfo();
  if ((proj_info == nullptr && other_proj_info != nullptr) ||
      (proj_info != nullptr && other_proj_info == nullptr)) {
    return false;
  }
  if (proj_info != nullptr && *proj_info != *other_proj_info) {
    return false;
  }

  // Looks okay, but let subclass make sure
  return true;
}

hash_t AbstractJoinPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  auto join_type = GetJoinType();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&join_type));

  if (GetPredicate() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  if (GetProjInfo() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetProjInfo()->Hash());
  }

  return hash;
}

}  // namespace planner
}  // namespace peloton
