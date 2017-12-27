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

namespace peloton {
namespace planner {

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

}  // namespace planner
}  // namespace peloton
