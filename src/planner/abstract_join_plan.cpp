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

  // Let the left guy populate the context with the attributes it produces
  BindingContext left_context;
  children[0]->PerformBinding(left_context);
  HandleSubplanBinding(/*is_left*/ true, left_context);

  // Let the right plan populate the context with the attributes it produces
  BindingContext right_context;
  children[1]->PerformBinding(right_context);
  HandleSubplanBinding(/*is_left*/ false, right_context);

  // Handle the projection
  std::vector<BindingContext *> input_contexts{&left_context, &right_context};
  GetProjInfo()->PerformRebinding(context, input_contexts);

  // Now we pull out all the attributes coming from each of the joins children
  std::vector<std::vector<oid_t>> input_cols;
  GetProjInfo()->PartitionInputs(input_cols);
  PL_ASSERT(input_cols.size() == 2);

  // Pull out the left and right non-key attributes
  const auto &left_inputs = input_cols[0];
  for (oid_t left_input_col : left_inputs) {
    left_attributes_.push_back(input_contexts[0]->Find(left_input_col));
  }
  const auto &right_inputs = input_cols[1];
  for (oid_t right_input_col : right_inputs) {
    right_attributes_.push_back(input_contexts[1]->Find(right_input_col));
  }

  // The predicate (if one exists) operates on the __output__ of the join and,
  // therefore, will bind against the resulting binding context
  const auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    const_cast<expression::AbstractExpression *>(predicate)
        ->PerformBinding(context);
  }
}

}  // namespace planner
}  // namespace peloton