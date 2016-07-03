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
#include "parser/peloton/create_parse.h"

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

}  // namespace planner
}  // namespace peloton
