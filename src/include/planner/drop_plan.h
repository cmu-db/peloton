//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// drop_plan.h
//
// Identification: /peloton/src/include/planner/drop_plan.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "storage/data_table.h"
#include "parser/peloton/drop_parse.h"

namespace peloton {


namespace planner {
class DropPlan : public AbstractPlan {
 public:

  DropPlan() = delete;
  DropPlan(const DropPlan &) = delete;
  DropPlan &operator=(const DropPlan &) = delete;
  DropPlan(DropPlan &&) = delete;
  DropPlan &operator=(DropPlan &&) = delete;

  explicit DropPlan(storage::DataTable *table) : target_table_(table) {}

  explicit DropPlan(std::string name) : table_name(name) {}

  explicit DropPlan(parser::DropParse *parse_tree) : table_name(parse_tree->GetEntityName()) {}

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_DROP;
  }

  const std::string GetInfo() const {
    std::string returned_string = "DropPlan:\n";
	returned_string += "\tTable name: " + table_name + "\n";
    return returned_string;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new DropPlan(target_table_));
  }

  std::string GetTableName() const { return table_name; }

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;
  std::string table_name;

};
}
}
