//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_plan.h
//
// Identification: src/include/planner/create_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "planner/abstract_plan.h"

namespace peloton {
namespace catalog {
class Schema;
}
namespace storage{
class DataTable;
}
namespace parser{
class CreateParse;
class CreateStatement;
}

namespace planner {
class CreatePlan : public AbstractPlan {
 public:

  CreatePlan() = delete;
  CreatePlan(const CreatePlan &) = delete;
  CreatePlan &operator=(const CreatePlan &) = delete;
  CreatePlan(CreatePlan &&) = delete;
  CreatePlan &operator=(CreatePlan &&) = delete;

  explicit CreatePlan(storage::DataTable *table);

  explicit CreatePlan(std::string name, std::unique_ptr<catalog::Schema> schema);

  explicit CreatePlan(parser::CreateParse *parse_tree);

  explicit CreatePlan(parser::CreateStatement *parse_tree);

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
