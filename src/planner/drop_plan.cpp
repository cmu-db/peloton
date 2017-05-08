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

#include "parser/drop_statement.h"
#include "storage/data_table.h"
#include "catalog/catalog.h"

namespace peloton {
namespace planner {

DropPlan::DropPlan(storage::DataTable *table) {
  target_table_ = table;
  missing = false;
}

DropPlan::DropPlan(std::string name) {
  table_name = name;
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, table_name);
  missing = false;
}

DropPlan::DropPlan(parser::DropStatement *parse_tree) {
  if (parse_tree->type == parser::DropStatement::EntityType::kTable) {
    table_name = parse_tree->GetTableName();
    // Set it up for the moment , cannot seem to find it in DropStatement
    missing = parse_tree->missing;

    try {
      target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
        parse_tree->GetDatabaseName(), table_name);
    }
    catch (CatalogException &e) {
      // Dropping a table which doesn't exist
      if (missing == false) {
        throw e;
      }
    }
  }
  else if (parse_tree->type == parser::DropStatement::EntityType::kTrigger) {
    // note parse_tree->table_name is different from parse_tree->GetTableName()
    table_name = std::string(parse_tree->table_name_of_trigger);
    trigger_name = std::string(parse_tree->trigger_name);
    drop_type = DropType::TRIGGER;
  }

}

}  // namespace planner
}  // namespace peloton
