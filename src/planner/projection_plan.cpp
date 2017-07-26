
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// projection_plan.cpp
//
// Identification: /peloton/src/planner/projection_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
  PL_ASSERT(children.size() == 1);

  // Let the child do its binding first
  BindingContext child_context;
  children[0]->PerformBinding(child_context);

  std::vector<const BindingContext *> inputs = {&child_context};
  GetProjectInfo()->PerformRebinding(context, inputs);
}

bool ProjectionPlan::Equals(planner::AbstractPlan &plan) const {
  if (GetPlanNodeType() != plan.GetPlanNodeType())
    return false;

  auto &other = reinterpret_cast<planner::ProjectionPlan &>(plan);
  // compare proj_info
  auto *proj = GetProjectInfo();
  auto *target_proj = other.GetProjectInfo();
  if (proj == nullptr && target_proj != nullptr)
    return false;
  if (!(proj == nullptr && target_proj == nullptr)) {
    if (!proj->Equals(*target_proj))
      return false;
  }

  // compare proj_schema
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

  return AbstractPlan::Equals(plan);
}

}  // namespace planner
}  // namespace peloton
