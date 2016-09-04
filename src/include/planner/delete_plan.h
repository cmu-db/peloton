//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.h
//
// Identification: src/include/planner/delete_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "common/types.h"
#include "parser/table_ref.h"
#include "catalog/schema.h"

namespace peloton {

namespace parser {
class DeleteStatement;
}
namespace storage {
class DataTable;
}
namespace expression {
class Expression;
}

namespace planner {

class DeletePlan : public AbstractPlan {
 public:
  DeletePlan() = delete;
  DeletePlan(const DeletePlan &) = delete;
  DeletePlan &operator=(const DeletePlan &) = delete;
  DeletePlan(DeletePlan &&) = delete;
  DeletePlan &operator=(DeletePlan &&) = delete;

  explicit DeletePlan(storage::DataTable *table, bool truncate);

  explicit DeletePlan(parser::DeleteStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_DELETE; }

  storage::DataTable *GetTable() const { return target_table_; }

  const std::string GetInfo() const { return "DeletePlan"; }

  void SetParameterValues(std::vector<common::Value *> *values);

  bool GetTruncate() const { return truncate; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new DeletePlan(target_table_, truncate));
  }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  std::string table_name_;

  expression::AbstractExpression *expr_ = nullptr;

  /** @brief Truncate table. */
  bool truncate = false;
};

}  // namespace planner
}  // namespace peloton
