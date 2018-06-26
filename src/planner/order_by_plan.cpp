//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_plan.cpp
//
// Identification: src/planner/order_by_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "planner/order_by_plan.h"

namespace peloton {
namespace planner {

OrderByPlan::OrderByPlan(const std::vector<oid_t> &sort_keys,
                         const std::vector<bool> &descend_flags,
                         const std::vector<oid_t> &output_column_ids)
    : sort_keys_(sort_keys),
      descend_flags_(descend_flags),
      output_column_ids_(output_column_ids),
      has_limit_(false),
      limit_(0),
      offset_(0) {}

OrderByPlan::OrderByPlan(const std::vector<oid_t> &sort_keys,
                         const std::vector<bool> &descend_flags,
                         const std::vector<oid_t> &output_column_ids,
                         const uint64_t limit, const uint64_t offset)
    : sort_keys_(sort_keys),
      descend_flags_(descend_flags),
      output_column_ids_(output_column_ids),
      has_limit_(true),
      limit_(limit),
      offset_(offset) {}

void OrderByPlan::PerformBinding(BindingContext &binding_context) {
  // Let the child do its binding first
  AbstractPlan::PerformBinding(binding_context);

  for (const oid_t col_id : GetOutputColumnIds()) {
    auto *ai = binding_context.Find(col_id);
    PELOTON_ASSERT(ai != nullptr);
    output_ais_.push_back(ai);
  }

  for (const oid_t sort_key_col_id : GetSortKeys()) {
    auto *ai = binding_context.Find(sort_key_col_id);
    PELOTON_ASSERT(ai != nullptr);
    sort_key_ais_.push_back(ai);
  }
}

const std::string OrderByPlan::GetInfo() const {
  return AbstractPlan::GetInfo();
}

std::unique_ptr<AbstractPlan> OrderByPlan::Copy() const {
  if (has_limit_) {
    return std::unique_ptr<AbstractPlan>(new OrderByPlan(
        sort_keys_, descend_flags_, output_column_ids_, limit_, offset_));
  } else {
    return std::unique_ptr<AbstractPlan>(
        new OrderByPlan(sort_keys_, descend_flags_, output_column_ids_));
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

  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&has_limit_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&limit_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&offset_));

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool OrderByPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  auto &other = static_cast<const planner::OrderByPlan &>(rhs);

  // Sort Keys
  if (GetSortKeys() != other.GetSortKeys()) {
    return false;
  }

  // Descend Flags
  if (GetDescendFlags() != other.GetDescendFlags()) {
    return false;
  }

  // Output Column Ids
  if (GetOutputColumnIds() != other.GetOutputColumnIds()) {
    return false;
  }

  // Limit/Offset
  if (HasLimit() != other.HasLimit() || GetOffset() != other.GetOffset() ||
      GetLimit() != other.GetLimit()) {
    return false;
  }

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
