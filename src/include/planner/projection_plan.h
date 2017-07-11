//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_plan.h
//
// Identification: src/include/planner/projection_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <numeric>

#include "planner/abstract_plan.h"
#include "planner/project_info.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace planner {

class ProjectionPlan : public AbstractPlan {
 public:
  ProjectionPlan(std::unique_ptr<const planner::ProjectInfo> &&project_info,
                 const std::shared_ptr<const catalog::Schema> &schema);

  // Rebind
  void PerformBinding(BindingContext &context) override;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  inline const catalog::Schema *GetSchema() const { return schema_.get(); }

  inline PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::PROJECTION;
  }

  const std::string GetInfo() const override { return "Projection"; }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns.resize(schema_->GetColumnCount());
    std::iota(columns.begin(), columns.end(), 0);
  }

  std::unique_ptr<AbstractPlan> Copy() const override {
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(schema_.get()));
    ProjectionPlan *new_plan =
        new ProjectionPlan(project_info_->Copy(), schema_copy);
    new_plan->column_ids_ = column_ids_;
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  /** @brief Projection Info.            */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /** @brief Schema of projected tuples. */
  std::shared_ptr<const catalog::Schema> schema_;

  /** @brief Columns involved */
  std::vector<oid_t> column_ids_;

 private:
  DISALLOW_COPY_AND_MOVE(ProjectionPlan);
};

}  // namespace planner
}  // namespace peloton