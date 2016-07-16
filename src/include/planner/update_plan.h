//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_plan.h
//
// Identification: src/include/planner/update_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "planner/abstract_plan.h"
#include "planner/project_info.h"
#include "common/types.h"
#include "parser/table_ref.h"

namespace peloton {

namespace parser{
  class UpdateStatement;
  class UpdateClause;
  class TableRef;
}

namespace expression {
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
                      std::unique_ptr<const planner::ProjectInfo> project_info);

  explicit UpdatePlan(parser::UpdateStatement* parse_tree);

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_UPDATE; }

  storage::DataTable *GetTable() const { return target_table_; }

  void ReplaceColumnExpressions(expression::AbstractExpression* expression);

  expression::AbstractExpression* ConvertToTupleValueExpression (std::string column_name);


  const std::string GetInfo() const { return "UpdatePlan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new UpdatePlan(target_table_, std::move(project_info_->Copy())));
  }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_;

  std::string table_name;

  /** @brief Projection info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  std::vector<parser::UpdateClause*>* updates;

  expression::AbstractExpression* where;

};

}  // namespace planner
}  // namespace peloton
