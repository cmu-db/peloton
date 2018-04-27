//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.cpp
//
// Identification: src/planner/seq_scan_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/seq_scan_plan.h"

#include "common/logger.h"
#include "common/macros.h"
#include "expression/abstract_expression.h"
#include "storage/data_table.h"
#include "common/internal_types.h"

namespace peloton {
namespace planner {

void SeqScanPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Sequential Scan");

  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

hash_t SeqScanPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());
  if (GetPredicate() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  for (auto &column_id : GetColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&column_id));
  }

  auto is_update = IsForUpdate();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_update));

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool SeqScanPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::SeqScanPlan &>(rhs);
  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PELOTON_ASSERT(table && other_table);
  if (*table != *other_table)
    return false;

  // Predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr))
    return false;
  if (pred && *pred != *other_pred)
    return false;

  // Column Ids
  size_t column_id_count = GetColumnIds().size();
  if (column_id_count != other.GetColumnIds().size())
    return false;
  for (size_t i = 0; i < column_id_count; i++) {
    if (GetColumnIds()[i] != other.GetColumnIds()[i]) {
      return false;
    }
  }

  if (IsForUpdate() != other.IsForUpdate())
    return false;

  return AbstractPlan::operator==(rhs);
}


void SeqScanPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  auto *predicate =
      const_cast<expression::AbstractExpression *>(GetPredicate());
  if (predicate != nullptr) {
    predicate->VisitParameters(map, values, values_from_user);
  }
}

}  // namespace planner
}  // namespace peloton
