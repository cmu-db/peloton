//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// delete_node.h
//
// Identification: src/backend/planner/delete_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "backend/planner/abstract_plan_node.h"
#include "backend/common/types.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace planner {

class DeleteNode : public AbstractPlanNode {
 public:
  DeleteNode() = delete;
  DeleteNode(const DeleteNode &) = delete;
  DeleteNode &operator=(const DeleteNode &) = delete;
  DeleteNode(DeleteNode &&) = delete;
  DeleteNode &operator=(DeleteNode &&) = delete;

  explicit DeleteNode(storage::DataTable *table, bool truncate)
      : target_table_(table), truncate(truncate) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_DELETE; }

  storage::DataTable *GetTable() const { return target_table_; }

  std::string GetInfo() const { return target_table_->GetName(); }

  bool GetTruncate() const { return truncate; }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_;

  /** @brief Truncate table. */
  bool truncate = false;
};

}  // namespace planner
}  // namespace peloton
