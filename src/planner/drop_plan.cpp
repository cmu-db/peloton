//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_plan.cpp
//
// Identification: src/planner/drop_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "planner/drop_plan.h"

#include "storage/data_table.h"
#include "parser/drop_parse.h"
#include "parser/statement_drop.h"
#include "catalog/bootstrapper.h"

namespace peloton {
namespace planner {

DropPlan::DropPlan(storage::DataTable *table) {
  target_table_ = table;
  missing = false;
}

DropPlan::DropPlan(std::string name) {
  table_name = name;
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME, table_name);
  missing = false;
}

DropPlan::DropPlan(parser::DropParse *parse_tree) {
  table_name = parse_tree->GetEntityName();
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME, table_name);
  missing = parse_tree->IsMissing();
}

DropPlan::DropPlan(parser::DropStatement *parse_tree) {
  table_name = parse_tree->name;
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME, table_name);
  // Set it up for the moment , cannot seem to find it in DropStatement
  missing = parse_tree->missing;
}

}  // namespace planner
}  // namespace peloton
