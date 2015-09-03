//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// projection_node.h
//
// Identification: src/backend/planner/projection_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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

  ProjectionPlan(const planner::ProjectInfo *project_info,
                 const catalog::Schema *schema)
      : project_info_(project_info), schema_(schema) {}

  inline const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  inline const catalog::Schema *GetSchema() const { return schema_.get(); }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_PROJECTION;
  }

  inline std::string GetInfo() const { return "Projection"; }

  void SetColumnIds(const std::vector<oid_t>& column_ids) {
    column_ids_ = column_ids;
  }

  const std::vector<oid_t>& GetColumnIds() const {
    return column_ids_;
  }

 private:
  /** @brief Projection Info.            */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /** @brief Schema of projected tuples. */
  std::unique_ptr<const catalog::Schema> schema_;

  /** @brief Columns involved */
  std::vector<oid_t> column_ids_;
};

}  // namespace planner
}  // namespace peloton
