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

hash_t ProjectionPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetProjectInfo()->Hash());

  auto schema = GetSchema();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&schema));

  for (const oid_t col_id : GetColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&col_id));
  }

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
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

void ProjectionPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  auto *proj_info = const_cast<planner::ProjectInfo *>(GetProjectInfo());
  proj_info->VisitParameters(map, values, values_from_user);
}

}  // namespace planner
}  // namespace peloton
