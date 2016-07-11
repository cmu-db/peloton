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

CreatePlan::CreatePlan(std::string name, std::unique_ptr<catalog::Schema> schema) {
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
  for(auto col : *parse_tree->columns){
    ValueType val = col->GetValueType(col->type);
    auto column = catalog::Column(val,GetTypeSize(val),std::string(col->name),false);
    columns.push_back(column);
  }
  catalog::Schema* schema = new catalog::Schema(columns);
  table_schema = schema;
}

}  // namespace planner
}  // namespace peloton
