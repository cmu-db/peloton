//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_plan.h
//
// Identification: src/include/planner/update_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/update_statement.h"
#include "planner/abstract_plan.h"
#include "planner/project_info.h"
#include "type/types.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class UpdatePlan : public AbstractPlan {
 public:
  UpdatePlan() = delete;

  UpdatePlan(storage::DataTable *table,
             std::unique_ptr<const planner::ProjectInfo> project_info);

  ~UpdatePlan() {}

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  storage::DataTable *GetTable() const { return target_table_; }

  bool GetUpdatePrimaryKey() const { return update_primary_key_; }

  void SetParameterValues(std::vector<type::Value> *values);

  PlanNodeType GetPlanNodeType() const { return PlanNodeType::UPDATE; }

  const std::string GetInfo() const { return "UpdatePlan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new UpdatePlan(target_table_, project_info_->Copy()));
  }

  bool Equals(planner::AbstractPlan &plan) const override;
  bool operator==(AbstractPlan &rhs) const override;
  bool operator!=(AbstractPlan &rhs) const override { return !(*this == rhs); }

 private:
  storage::DataTable *target_table_;

  std::unique_ptr<const planner::ProjectInfo> project_info_;

  bool update_primary_key_;

 private:
  DISALLOW_COPY_AND_MOVE(UpdatePlan);
};

}  // namespace planner
}  // namespace peloton
