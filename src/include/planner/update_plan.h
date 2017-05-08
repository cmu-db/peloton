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

#include "../parser/update_statement.h"
#include "planner/abstract_plan.h"
#include "planner/project_info.h"
#include "type/types.h"
#include "parser/table_ref.h"
#include "catalog/schema.h"

namespace peloton {

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

  explicit UpdatePlan(storage::DataTable *table,
                      std::unique_ptr<const planner::ProjectInfo> project_info);

  // FIXME: Should remove when the simple_optimizer tears down
  explicit UpdatePlan(const parser::UpdateStatement *parse_tree);

  // FIXME: Should remove when the simple_optimizer tears down
  explicit UpdatePlan(const parser::UpdateStatement *parse_tree,
                      std::vector<oid_t> &key_column_ids,
                      std::vector<ExpressionType> &expr_types,
                      std::vector<type::Value> &values, oid_t &index_id);

  inline ~UpdatePlan() {
    if (where_ != nullptr) {
      delete (where_);
    }

    for (size_t update_itr = 0; update_itr < updates_.size(); ++update_itr) {
      delete (updates_[update_itr]);
    }
  }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::UPDATE; }

  storage::DataTable *GetTable() const { return target_table_; }

  const std::string GetInfo() const { return "UpdatePlan"; }

  void SetParameterValues(std::vector<type::Value> *values);

  bool GetUpdatePrimaryKey() const { return update_primary_key_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new UpdatePlan(target_table_, std::move(project_info_->Copy())));
  }

  // Bindings
  void PerformBinding(BindingContext &binding_context) override;

 private:
  // Initialize private members and construct colum_ids given a UpdateStatement.
  void BuildInitialUpdatePlan(const parser::UpdateStatement *parse_tree,
                              std::vector<oid_t> &columns);

  /** @brief Target table. */
  storage::DataTable *target_table_;

  /** @brief Projection info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  // FIXME: Should remove when the simple_optimizer tears down
  // Vector of Update clauses
  std::vector<parser::UpdateClause *> updates_;

  // FIXME: Should remove when the simple_optimizer tears down
  // The where condition
  expression::AbstractExpression *where_;

  // Whether update primary key
  bool update_primary_key_;

 private:
  DISALLOW_COPY_AND_MOVE(UpdatePlan);
};

}  // namespace planner
}  // namespace peloton
