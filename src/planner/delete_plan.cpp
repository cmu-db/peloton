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
#include "parser/statement_delete.h"
#include "expression/expression_util.h"
#include "catalog/catalog.h"

namespace peloton {

namespace expression {
class TupleValueExpression;
}
namespace planner {

DeletePlan::DeletePlan(storage::DataTable *table, bool truncate)
    : target_table_(table), truncate(truncate) {
  LOG_TRACE("Creating a Delete Plan");
}

DeletePlan::DeletePlan(parser::DeleteStatement *delete_statemenet) {

  LOG_TRACE("Creating a Delete Plan");
  table_name_ = std::string(delete_statemenet->table_name);
  target_table_ = catalog::Catalog::GetInstance()->GetTableFromDatabase(
      DEFAULT_DB_NAME, table_name_);
  // if expr is null , delete all tuples from table
  if (delete_statemenet->expr == nullptr) {
    LOG_TRACE("No expression, setting truncate to true");
    expr_ = nullptr;
    truncate = true;

  } else {
    expr_ = delete_statemenet->expr->Copy();
    LOG_TRACE("Replacing COLUMN_REF with TupleValueExpressions");
    ReplaceColumnExpressions(target_table_->GetSchema(), expr_);
  }
  std::vector<oid_t> column_ids = {};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table_, expr_, column_ids));
  AddChild(std::move(seq_scan_node));
}

void DeletePlan::SetParameterValues(std::vector<Value> *values) {
  LOG_TRACE("Setting parameter values in Delete");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

}  // namespace planner
}  // namespace peloton
