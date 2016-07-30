//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_plan.cpp
//
// Identification: src/planner/create_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/create_plan.h"

#include "storage/data_table.h"
#include "catalog/bootstrapper.h"
#include "parser/create_parse.h"
#include "parser/statement_create.h"

namespace peloton {
namespace planner {

CreatePlan::CreatePlan(storage::DataTable *table) {
  target_table_ = table;
  table_schema = nullptr;
}

CreatePlan::CreatePlan(std::string name,
                       std::unique_ptr<catalog::Schema> schema) {
  table_name = name;
  table_schema = schema.release();
}

CreatePlan::CreatePlan(parser::CreateParse *parse_tree) {
  table_name = parse_tree->GetTableName();
  table_schema = parse_tree->GetSchema();
}

CreatePlan::CreatePlan(parser::CreateStatement *parse_tree) {
  table_name = std::string(parse_tree->name);
  std::vector<catalog::Column> columns;
  std::vector<catalog::Constraint> column_contraints;
  for (auto col : *parse_tree->columns) {
    ValueType val = col->GetValueType(col->type);

    LOG_INFO("Column name: %s; Is primary key: %d", col->name, col->primary);

    // Check main constraints
    if (col->primary) {
      catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, "con_primary");
      column_contraints.push_back(constraint);
      LOG_INFO("Added a primary key constraint on column \"%s\"", col->name);
    }

    if (col->not_null) {
      catalog::Constraint constraint(CONSTRAINT_TYPE_NOTNULL, "con_not_null");
      column_contraints.push_back(constraint);
    }

    auto column =
        catalog::Column(val, GetTypeSize(val), std::string(col->name), false);
    for (auto con : column_contraints) {
      column.AddConstraint(con);
    }

    column_contraints.clear();
    columns.push_back(column);
  }
  catalog::Schema *schema = new catalog::Schema(columns);
  table_schema = schema;
}

}  // namespace planner
}  // namespace peloton
