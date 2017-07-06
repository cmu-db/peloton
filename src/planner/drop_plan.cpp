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

DropPlan::DropPlan(std::string name, concurrency::Transaction *consistentTxn) {
  table_name = name;
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, table_name, consistentTxn);
  missing = false;
}

DropPlan::DropPlan(parser::DropStatement *parse_tree,
                   concurrency::Transaction *consistentTxn) {
  table_name = parse_tree->GetTableName();
  // Set it up for the moment , cannot seem to find it in DropStatement
  missing = parse_tree->missing;

  try {
    target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
        parse_tree->GetDatabaseName(), table_name, consistentTxn);
  } catch (CatalogException &e) {
    // Dropping a table which doesn't exist
    if (missing == false) {
      throw e;
    }
  }
}

}  // namespace planner
}  // namespace peloton
