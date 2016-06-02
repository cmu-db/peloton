//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_plan.h
//
// Identification: src/backend/planner/projection_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "abstract_plan.h"
#include "backend/common/types.h"
#include "backend/planner/project_info.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace planner {

class ProjectionPlan : public AbstractPlan {
 public:
  ProjectionPlan(const ProjectionPlan &) = delete;
  ProjectionPlan &operator=(const ProjectionPlan &) = delete;
  ProjectionPlan(ProjectionPlan &&) = delete;
  ProjectionPlan &operator=(ProjectionPlan &&) = delete;

  ProjectionPlan(std::unique_ptr<const planner::ProjectInfo> &&project_info,
                 std::shared_ptr<const catalog::Schema> &schema)
      : project_info_(std::move(project_info)), schema_(schema) {}

  inline const planner::ProjectInfo *GetProjectInfo()
      const {
    return project_info_.get();
  }

  inline const catalog::Schema *GetSchema() const {
    return schema_.get();
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_PROJECTION;
  }

  const std::string GetInfo() const { return "Projection"; }

  void SetColumnIds(const std::vector<oid_t> &column_ids) {
    column_ids_ = column_ids;
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(schema_.get()));
    ProjectionPlan *new_plan =
        new ProjectionPlan(project_info_->Copy(), schema_copy);
    new_plan->SetColumnIds(column_ids_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  /** @brief Projection Info.            */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /** @brief Schema of projected tuples. */
  std::shared_ptr<const catalog::Schema> schema_;

  /** @brief Columns involved */
  std::vector<oid_t> column_ids_;
};

}  // namespace planner
}  // namespace peloton
