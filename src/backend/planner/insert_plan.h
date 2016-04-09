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
                      const planner::ProjectInfo *project_info,
                      storage::Tuple *tuple = nullptr,
                      oid_t bulk_insert_count = 1)
      : target_table_(table),
        project_info_(project_info),
        tuple_(std::unique_ptr<storage::Tuple>(tuple)),
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
