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

  // This constructor takes in neither a project info nor a tuple
  // It must be used when the input is a logical tile
  explicit InsertPlan(storage::DataTable *table,
                      oid_t bulk_insert_count = 1)
      : target_table_(table),
        bulk_insert_count(bulk_insert_count) {}

  // This constructor takes in a project info
  explicit InsertPlan(storage::DataTable *table,
                      std::unique_ptr<const planner::ProjectInfo> &&project_info,
                      oid_t bulk_insert_count = 1)
      : target_table_(table),
        project_info_(std::move(project_info)),
        bulk_insert_count(bulk_insert_count) {}

  // This constructor takes in a tuple
  explicit InsertPlan(storage::DataTable *table,
                      std::unique_ptr<storage::Tuple> &&tuple,
                      oid_t bulk_insert_count = 1)
      : target_table_(table),
        tuple_(std::move(tuple)),
        bulk_insert_count(bulk_insert_count) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_INSERT; }

  storage::DataTable *GetTable() const { return target_table_; }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  oid_t GetBulkInsertCount() const { return bulk_insert_count; }

  const storage::Tuple *GetTuple() const {
    return tuple_.get();
  }

  const std::string GetInfo() const { return "InsertPlan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    // TODO: Fix tuple copy
    return std::unique_ptr<AbstractPlan>(new InsertPlan(
        target_table_,
        std::move(project_info_->Copy())));
  }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Projection Info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /** @brief Tuple */
  std::unique_ptr<storage::Tuple> tuple_;

  /** @brief Number of times to insert */
  oid_t bulk_insert_count;

};

}  // namespace planner
}  // namespace peloton
