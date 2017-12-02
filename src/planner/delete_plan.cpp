//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.cpp
//
// Identification: src/planner/delete_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/delete_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

DeletePlan::DeletePlan(storage::DataTable *table) : target_table_(table) {
  LOG_TRACE("Creating a Delete Plan");
}

void DeletePlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Delete");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

hash_t DeletePlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool DeletePlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::DeletePlan &>(rhs);

  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PL_ASSERT(table && other_table);
  if (*table != *other_table)
    return false;

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
