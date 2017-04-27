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

#include "catalog/schema.h"
#include "type/types.h"
#include "parser/table_ref.h"
#include "planner/abstract_plan.h"

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

/**
 * @brief The @c DELETE physical plan.
 *
 *
 */
class DeletePlan : public AbstractPlan {
 public:
  DeletePlan() = delete;

  ~DeletePlan() {
    if (expr_ != nullptr) {
      delete expr_;
    }
  }

  explicit DeletePlan(storage::DataTable *table, bool truncate);

  explicit DeletePlan(storage::DataTable *table,
                      const expression::AbstractExpression *predicate);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::DELETE; }

  storage::DataTable *GetTable() const { return target_table_; }

  const std::string GetInfo() const { return "DeletePlan"; }

  void SetParameterValues(std::vector<type::Value> *values) override;

  bool GetTruncate() const { return truncate; }

  expression::AbstractExpression *GetPredicate() { return expr_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new DeletePlan(target_table_, truncate));
  }

 private:
  void BuildInitialDeletePlan(parser::DeleteStatement *delete_statemenet);

  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  // TODO: should be deleted after refacor
  std::string table_name_;

  expression::AbstractExpression *expr_ = nullptr;

  /** @brief Truncate table. */
  bool truncate = false;

 private:
  DISALLOW_COPY_AND_MOVE(DeletePlan);
};

}  // namespace planner
}  // namespace peloton
