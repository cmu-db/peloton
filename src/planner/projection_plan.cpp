
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

}  // namespace planner
}  // namespace peloton
