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

  explicit CreatePlan(std::string name, std::unique_ptr<catalog::Schema> schema, CreateType c_type);

  explicit CreatePlan(parser::CreateParse *parse_tree);

  explicit CreatePlan(parser::CreateStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_CREATE;
  }

  const std::string GetInfo() const {
    return "Create Plan";
  }

  void SetParameterValues(UNUSED_ATTRIBUTE std::vector<Value>* values) { };

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new CreatePlan(target_table_));
  }

  std::string GetIndexName() const {return index_name;}

  std::string GetTableName() const { return table_name; }

  catalog::Schema* GetSchema() const {
    return table_schema;
  }

  CreateType GetCreateType() const { return create_type; }

  bool IsUnique() const { return unique; }

  std::vector<std::string> GetIndexAttributes() const { return index_attrs;}

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;

  std::string table_name;

  catalog::Schema* table_schema;

  std::vector<std::string> index_attrs;
  
  CreateType create_type;
  // IndexName
  std::string index_name;

  bool unique;
};
}
}
