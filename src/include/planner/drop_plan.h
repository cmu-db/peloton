//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_plan.h
//
// Identification: src/include/planner/drop_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace storage {
class DataTable;
}
namespace parser {
class DropStatement;
}
namespace catalog {
class Schema;
}

namespace planner {

class DropPlan : public AbstractPlan {
 public:
  DropPlan() = delete;

  explicit DropPlan(storage::DataTable *table);
  // # 623
  explicit DropPlan(std::string name,
                    concurrency::Transaction *consistentTxn = nullptr);

  explicit DropPlan(parser::DropStatement *parse_tree,
                    concurrency::Transaction *consistentTxn = nullptr);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::DROP; }

  const std::string GetInfo() const {
    std::string returned_string = "DropPlan:\n";
    returned_string += "\tTable name: " + table_name + "\n";
    return returned_string;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new DropPlan(target_table_));
  }

  std::string GetTableName() const { return table_name; }

  bool IsMissing() const { return missing; }

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;
  std::string table_name;
  bool missing;

 private:
  DISALLOW_COPY_AND_MOVE(DropPlan);
};

}  // namespace planner
}  // namespace peloton