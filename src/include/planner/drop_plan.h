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

#include "concurrency/transaction_context.h"
#include "planner/abstract_plan.h"

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

  explicit DropPlan(const std::string &name);

  explicit DropPlan(parser::DropStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::DROP; }

  const std::string GetInfo() const {
    std::string returned_string = "DropPlan:\n";
    returned_string += " Table name:     " + table_name;
    returned_string += " Database name : " + database_name;
    return returned_string;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new DropPlan(table_name));
  }

  std::string GetDatabaseName() const { return database_name; }

  std::string GetTableName() const { return table_name; }

  std::string GetTriggerName() const { return trigger_name; }

  std::string GetIndexName() const { return index_name; }

  DropType GetDropType() const { return drop_type; }

  bool IsMissing() const { return missing; }

 private:
  DropType drop_type = DropType::TABLE;

  // Target Table
  std::string table_name;

  // Database Name
  std::string database_name;

  std::string trigger_name;
  std::string index_name;
  bool missing;

 private:
  DISALLOW_COPY_AND_MOVE(DropPlan);
};

}  // namespace planner
}  // namespace peloton
