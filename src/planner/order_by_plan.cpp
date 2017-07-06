//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_plan.cpp
//
// Identification: src/planner/order_by_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "planner/order_by_plan.h"

#include "expression/abstract_expression.h"

namespace peloton {
namespace planner {

OrderByPlan::OrderByPlan(const std::vector<oid_t> &sort_keys,
                         const std::vector<bool> &descend_flags,
                         const std::vector<oid_t> &output_column_ids)
    : sort_keys_(sort_keys),
      descend_flags_(descend_flags),
      output_column_ids_(output_column_ids) {}

void OrderByPlan::PerformBinding(BindingContext &binding_context) {
  // Let the child do its binding first
  AbstractPlan::PerformBinding(binding_context);

  for (const oid_t col_id : GetOutputColumnIds()) {
    auto *ai = binding_context.Find(col_id);
    PL_ASSERT(ai != nullptr);
    output_ais_.push_back(ai);
  }

  for (const oid_t sort_key_col_id : GetSortKeys()) {
    auto *ai = binding_context.Find(sort_key_col_id);
    PL_ASSERT(ai != nullptr);
    sort_key_ais_.push_back(ai);
  }
}

}  // namespace planner
}  // namespace peloton