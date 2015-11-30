//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// delete_node.h
//
// Identification: src/backend/planner/delete_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "backend/common/types.h"

namespace peloton {

namespace storage{
class DataTable;
}

namespace planner {

class DeletePlan : public AbstractPlan {
 public:
  DeletePlan() = delete;
  DeletePlan(const DeletePlan &) = delete;
  DeletePlan &operator=(const DeletePlan &) = delete;
  DeletePlan(DeletePlan &&) = delete;
  DeletePlan &operator=(DeletePlan &&) = delete;

  explicit DeletePlan(storage::DataTable *table, bool truncate)
      : target_table_(table), truncate(truncate) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_DELETE; }

  storage::DataTable *GetTable() const {
    return target_table_;
  }

  std::string GetInfo() const { return "DeletePlan"; }

  bool GetTruncate() const { return truncate; }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Truncate table. */
  bool truncate = false;
};

}  // namespace planner
}  // namespace peloton
