//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// update_node.h
//
// Identification: src/backend/planner/update_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "backend/planner/project_info.h"
#include "backend/common/types.h"

namespace peloton {

namespace expression{
class Expression;
}

namespace storage {
class DataTable;
}

namespace planner {

class UpdatePlan : public AbstractPlan {
 public:
  UpdatePlan() = delete;
  UpdatePlan(const UpdatePlan &) = delete;
  UpdatePlan &operator=(const UpdatePlan &) = delete;
  UpdatePlan(UpdatePlan &&) = delete;
  UpdatePlan &operator=(UpdatePlan &&) = delete;

  explicit UpdatePlan(storage::DataTable *table,
                      const planner::ProjectInfo *project_info)
      : target_table_(table), project_info_(project_info) {}

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_UPDATE; }

  storage::DataTable *GetTable() const { return target_table_; }

  std::string GetInfo() const { return "UpdatePlan"; }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_;

  /** @brief Projection info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;
};

}  // namespace planner
}  // namespace peloton
