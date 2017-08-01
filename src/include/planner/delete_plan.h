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

#include "catalog/schema.h"
#include "type/types.h"
#include "parser/table_ref.h"
#include "planner/abstract_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace expression {
class AbstractExpression;
}
namespace concurrency {
class Transaction;
}

namespace planner {

class DeletePlan : public AbstractPlan {
 public:
  DeletePlan() = delete;

  ~DeletePlan() {
    if (predicate_ != nullptr) {
      delete predicate_;
    }
  }

  DeletePlan(storage::DataTable *table, bool truncate);

  DeletePlan(storage::DataTable *table,
             const expression::AbstractExpression *predicate);

  storage::DataTable *GetTable() const { return target_table_; }

  bool GetTruncate() const { return truncate_; }

  void SetParameterValues(std::vector<type::Value> *values) override;

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  const std::string GetInfo() const override { return "DeletePlan"; }

  std::unique_ptr<AbstractPlan> Copy() const override {
    return std::unique_ptr<AbstractPlan>(
        new DeletePlan(target_table_, truncate_));
  }

 private:
  storage::DataTable *target_table_ = nullptr;

  expression::AbstractExpression *predicate_ = nullptr;

  bool truncate_ = false;

 private:
  DISALLOW_COPY_AND_MOVE(DeletePlan);
};

}  // namespace planner
}  // namespace peloton
