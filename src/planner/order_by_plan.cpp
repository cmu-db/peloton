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
    auto* ai = binding_context.Find(col_id);
    PL_ASSERT(ai != nullptr);
    output_ais_.push_back(ai);
  }

  for (const oid_t sort_key_col_id : GetSortKeys()) {
    auto* ai = binding_context.Find(sort_key_col_id);
    PL_ASSERT(ai != nullptr);
    sort_key_ais_.push_back(ai);
  }
}

hash_t OrderByPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  for (const oid_t sort_key : GetSortKeys()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&sort_key));
  }

  for (const bool flag : GetDescendFlags()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&flag));
  }

  for (const oid_t col_id : GetOutputColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&col_id));
  }

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool OrderByPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::OrderByPlan &>(rhs);

  // Sort Keys 
  size_t sort_keys_count = GetSortKeys().size();
  if (sort_keys_count != other.GetSortKeys().size())
    return false;

  for (size_t i = 0; i < sort_keys_count; i++) {
    if (GetSortKeys()[i] != other.GetSortKeys()[i])
      return false;
  }

  // Descend Flags
  size_t descend_flags_count = GetDescendFlags().size();
  if (descend_flags_count != other.GetDescendFlags().size())
    return false;

  for (size_t i = 0; i < descend_flags_count; i++) {
    if (GetDescendFlags()[i] != other.GetDescendFlags()[i])
      return false;
  }

  // Output Column Ids
  size_t column_id_count = GetOutputColumnIds().size();
  if (column_id_count != other.GetOutputColumnIds().size())
    return false;

  for (size_t i = 0; i < column_id_count; i++) {
    if (GetOutputColumnIds()[i] != other.GetOutputColumnIds()[i])
      return false;
  }

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
