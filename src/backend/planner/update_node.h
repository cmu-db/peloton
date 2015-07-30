/**
 * @brief Header for delete plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/planner/abstract_plan_node.h"

#include "backend/planner/project_info.h"
#include "backend/expression/abstract_expression.h"
#include "backend/common/types.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace planner {

class UpdateNode : public AbstractPlanNode {
 public:
  UpdateNode() = delete;
  UpdateNode(const UpdateNode &) = delete;
  UpdateNode &operator=(const UpdateNode &) = delete;
  UpdateNode(UpdateNode &&) = delete;
  UpdateNode &operator=(UpdateNode &&) = delete;

  explicit UpdateNode(storage::DataTable *table,
                      const planner::ProjectInfo *project_info)
      : target_table_(table), project_info_(project_info) {}

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_UPDATE; }

  storage::DataTable *GetTable() const { return target_table_; }

  std::string GetInfo() const { return target_table_->GetName(); }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_;

  /** @brief Projection info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;
};

}  // namespace planner
}  // namespace peloton
