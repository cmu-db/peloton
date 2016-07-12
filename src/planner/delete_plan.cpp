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


#include "planner/delete_plan.h"

#include "storage/data_table.h"
#include "catalog/bootstrapper.h"
#include "parser/statement_delete.h"

namespace peloton {
namespace planner {

DeletePlan::DeletePlan(storage::DataTable *table, bool truncate)
      : target_table_(table), truncate(truncate) {}

DeletePlan::DeletePlan(parser::DeleteStatement *parse_tree) {
  // Add your stuff here
  table_name = std::string(parse_tree->table_name);
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME, table_name);
  // if expr is null , delete all tuples from table
  if(parse_tree->expr == NULL) {
	  truncate = true;
  }
  else {
	  expr = parse_tree->expr;
	  std::vector<oid_t> column_ids = {};
	  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
	      new planner::SeqScanPlan(target_table_, expr, column_ids));
	  AddChild(std::move(seq_scan_node));
  }


}

}  // namespace planner
}  // namespace peloton
