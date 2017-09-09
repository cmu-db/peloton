//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_plan.cpp
//
// Identification: src/planner/projection_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/projection_plan.h"

#include "common/logger.h"

namespace peloton {
namespace planner {

ProjectionPlan::ProjectionPlan(
    std::unique_ptr<const planner::ProjectInfo> &&project_info,
    const std::shared_ptr<const catalog::Schema> &schema)
    : project_info_(std::move(project_info)), schema_(schema) {}

void ProjectionPlan::PerformBinding(BindingContext &context) {
  const auto& children = GetChildren();
  if (children.empty())
    return;
  PL_ASSERT(children.size() == 1);

  // Let the child do its binding first
  BindingContext child_context;
  children[0]->PerformBinding(child_context);

  std::vector<const BindingContext *> inputs = {&child_context};
  GetProjectInfo()->PerformRebinding(context, inputs);
}

bool ProjectionPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::ProjectionPlan &>(rhs);
  // compare proj_info
  auto *proj_info = GetProjectInfo();
  auto *other_proj_info = other.GetProjectInfo();
  if ((proj_info == nullptr && other_proj_info != nullptr) ||
      (proj_info != nullptr && other_proj_info == nullptr))
    return false;
  if (proj_info && *proj_info != *other_proj_info)
    return false;

  if (*GetSchema() == *other.GetSchema())
    return false;

  // compare column_ids
  size_t column_id_count = GetColumnIds().size();
  if (column_id_count != other.GetColumnIds().size())
    return false;
  for (size_t i = 0; i < column_id_count; i++) {
    if (GetColumnIds()[i] != other.GetColumnIds()[i])
      return false;
  }

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
