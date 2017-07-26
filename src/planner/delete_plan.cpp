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

bool DeletePlan::Equals(planner::AbstractPlan &plan) const {
  if (GetPlanNodeType() != plan.GetPlanNodeType())
    return false;

  auto &other = reinterpret_cast<planner::DeletePlan &>(plan);
  // compare table
  if (!GetTable()->Equals(*other.GetTable()))
    return false;

  // compare truncate
  if (GetTruncate() != other.GetTruncate())
    return false;

  return AbstractPlan::Equals(plan);
}

}  // namespace planner
}  // namespace peloton
