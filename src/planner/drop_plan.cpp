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

#include "catalog/catalog.h"
#include "parser/drop_statement.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

DropPlan::DropPlan(std::string name) {
  table_name = name;
  missing = false;
}

DropPlan::DropPlan(parser::DropStatement *parse_tree) {
  if (parse_tree->type == parser::DropStatement::EntityType::kTable) {
    table_name = parse_tree->GetTableName();
    missing = parse_tree->missing;
  } else if (parse_tree->type == parser::DropStatement::EntityType::kTrigger) {
    // note parse_tree->table_name is different from parse_tree->GetTableName()
    table_name = std::string(parse_tree->table_name_of_trigger);
    trigger_name = std::string(parse_tree->trigger_name);
    drop_type = DropType::TRIGGER;
  }
}

}  // namespace planner
}  // namespace peloton
