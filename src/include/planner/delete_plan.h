//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.h
//
// Identification: src/include/planner/delete_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "common/internal_types.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class DeletePlan : public AbstractPlan {
 public:
  DeletePlan() = delete;

  ~DeletePlan() {}

  DeletePlan(storage::DataTable *table);

  storage::DataTable *GetTable() const { return target_table_; }

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  const std::string GetInfo() const override { return "DeletePlan"; }

  void SetParameterValues(std::vector<type::Value> *values) override;

  std::unique_ptr<AbstractPlan> Copy() const override {
    return std::unique_ptr<AbstractPlan>(new DeletePlan(target_table_));
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

 private:
  storage::DataTable *target_table_ = nullptr;

 private:
  DISALLOW_COPY_AND_MOVE(DeletePlan);
};

}  // namespace planner
}  // namespace peloton
