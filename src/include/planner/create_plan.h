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
namespace storage {
class DataTable;
}
namespace parser {
class CreateStatement;
}

namespace planner {

class CreatePlan : public AbstractPlan {
 public:
  CreatePlan() = delete;

  explicit CreatePlan(storage::DataTable *table);

  explicit CreatePlan(std::string name, std::string database_name,
                      std::unique_ptr<catalog::Schema> schema,
                      CreateType c_type);

  explicit CreatePlan(parser::CreateStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::CREATE; }

  const std::string GetInfo() const { return "Create Plan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new CreatePlan(target_table_));
  }

  std::string GetIndexName() const { return index_name; }

  std::string GetTableName() const { return table_name; }

  std::string GetDatabaseName() const { return database_name; }

  catalog::Schema *GetSchema() const { return table_schema; }

  CreateType GetCreateType() const { return create_type; }

  bool IsUnique() const { return unique; }

  IndexType GetIndexType() const { return index_type; }

  std::vector<std::string> GetIndexAttributes() const { return index_attrs; }

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;

  // Table Name
  std::string table_name;

  // Database Name
  std::string database_name;

  // Table Schema
  catalog::Schema *table_schema;

  // Index attributes
  std::vector<std::string> index_attrs;

  // Check to either Create Table or INDEX
  CreateType create_type;

  // IndexName
  std::string index_name;

  // Index Tyoe specified from parser (Default: SKIPLIST)
  IndexType index_type;

  // UNIQUE INDEX flag
  bool unique;

 private:
  DISALLOW_COPY_AND_MOVE(CreatePlan);
};

}  // namespace planner
}  // namespace peloton