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

#include "expression/parameter.h"
#include "parser/update_statement.h"
#include "planner/abstract_plan.h"
#include "planner/project_info.h"
#include "common/internal_types.h"

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

  void SetParameterValues(std::vector<type::Value> *values) override;

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UPDATE; }

  const std::string GetInfo() const override { return "UpdatePlan"; }

  std::unique_ptr<AbstractPlan> Copy() const override {
    return std::unique_ptr<AbstractPlan>(
        new UpdatePlan(target_table_, project_info_->Copy()));
  }

  void PerformBinding(BindingContext &binding_context) override;

  const std::vector<const AttributeInfo *> &GetAttributeInfos() const {
    return ais_;
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  storage::DataTable *target_table_;

  std::unique_ptr<const planner::ProjectInfo> project_info_;

  bool update_primary_key_;

  std::vector<const AttributeInfo *> ais_;

 private:
  DISALLOW_COPY_AND_MOVE(UpdatePlan);
};

}  // namespace planner
}  // namespace peloton
