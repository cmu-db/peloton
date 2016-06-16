//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// create_plan.h
//
// Identification: /peloton/src/include/planner/create_plan.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

#include "storage/data_table.h"
#include "parser/peloton/create_parse.h"

namespace peloton {

namespace storage{
class DataTable;
}

namespace planner {
class CreatePlan : public AbstractPlan {
 public:

  CreatePlan() = delete;
  CreatePlan(const CreatePlan &) = delete;
  CreatePlan &operator=(const CreatePlan &) = delete;
  CreatePlan(CreatePlan &&) = delete;
  CreatePlan &operator=(CreatePlan &&) = delete;

  explicit CreatePlan(storage::DataTable *table) {
	  target_table_ = table;
	  table_schema = nullptr;
  }

  explicit CreatePlan(std::string name, std::unique_ptr<catalog::Schema> schema) {
    table_name = name;
    table_schema = schema.release();
  }

  explicit CreatePlan(parser::CreateParse *parse_tree){
    table_name = parse_tree->GetTableName();
    table_schema = &parse_tree->GetSchema();
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_CREATE;
  }

  const std::string GetInfo() const {
    return "Create Plan";
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new CreatePlan(target_table_));
  }

  std::string GetTableName() const { return table_name; }

  catalog::Schema* GetSchema() const {
	  return table_schema;
  }

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;
  std::string table_name;
  catalog::Schema* table_schema;

};
}
}
