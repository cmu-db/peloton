//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_node.h
//
// Identification: src/backend/planner/insert_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "backend/planner/project_info.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace planner {

class InsertPlan : public AbstractPlan {
 public:
  InsertPlan() = delete;
  InsertPlan(const InsertPlan &) = delete;
  InsertPlan &operator=(const InsertPlan &) = delete;
  InsertPlan(InsertPlan &&) = delete;
  InsertPlan &operator=(InsertPlan &&) = delete;

  explicit InsertPlan(storage::DataTable *table,
                      const planner::ProjectInfo *project_info)
      : target_table_(table), project_info_(project_info) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_INSERT; }

  storage::DataTable *GetTable() const {
    return target_table_;
  }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  std::string GetInfo() const { return target_table_->GetName(); }

 private:

  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Projection Info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;
};

}  // namespace planner
}  // namespace peloton
