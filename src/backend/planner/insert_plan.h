//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_plan.h
//
// Identification: src/backend/planner/insert_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "backend/planner/project_info.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class InsertPlan : public AbstractPlan {
 public:
  InsertPlan() = delete;
  InsertPlan(const InsertPlan &) = delete;
  InsertPlan &operator=(const InsertPlan &) = delete;
  InsertPlan(InsertPlan &&) = delete;
  InsertPlan &operator=(InsertPlan &&) = delete;

  explicit InsertPlan(storage::DataTable *table,
                      std::unique_ptr<const planner::ProjectInfo> &&project_info,
                      oid_t bulk_insert_count = 1)
      : target_table_(table),
        project_info_(std::move(project_info)),
        bulk_insert_count(bulk_insert_count) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_INSERT; }

  storage::DataTable *GetTable() const { return target_table_; }

  const std::unique_ptr<const planner::ProjectInfo> &GetProjectInfo() const {
    return project_info_;
  }

  oid_t GetBulkInsertCount() const { return bulk_insert_count; }

  const std::string GetInfo() const { return "InsertPlan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new InsertPlan(
        target_table_, std::move(project_info_->Copy()), bulk_insert_count));
  }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Projection Info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  // Number of times to insert
  oid_t bulk_insert_count;
};

}  // namespace planner
}  // namespace peloton
