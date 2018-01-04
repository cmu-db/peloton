//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_plan.cpp
//
// Identification: src/planner/drop_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/drop_plan.h"

#include "catalog/catalog.h"
#include "parser/drop_statement.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

DropPlan::DropPlan(const std::string &name) {
  table_name = name;
  missing = false;
}

DropPlan::DropPlan(parser::DropStatement *parse_tree) {
  switch (parse_tree->GetDropType()) {
    case parser::DropStatement::EntityType::kDatabase: {
      database_name = parse_tree->GetDatabaseName();
      missing = parse_tree->GetMissing();
      drop_type = DropType::DB;
      break;
    }
    case parser::DropStatement::EntityType::kTable: {
      database_name = parse_tree->GetDatabaseName();
      table_name = parse_tree->GetTableName();
      missing = parse_tree->GetMissing();
      drop_type = DropType::TABLE;
      break;
    }
    case parser::DropStatement::EntityType::kTrigger: {
      // note parse_tree->table_name is different from
      // parse_tree->GetTableName()
      database_name = parse_tree->GetDatabaseName();
      table_name = std::string(parse_tree->GetTriggerTableName());
      trigger_name = std::string(parse_tree->GetTriggerName());
      drop_type = DropType::TRIGGER;
      break;
    }
    case parser::DropStatement::EntityType::kIndex: {
      index_name = std::string(parse_tree->GetIndexName());
      drop_type = DropType::INDEX;
      break;
    }
    default: { LOG_ERROR("Not supported Drop type"); }
  }
}

}  // namespace planner
}  // namespace peloton
