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

#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {

namespace expression {
class TupleValueExpression;
}
namespace planner {

DeletePlan::DeletePlan(storage::DataTable *table, bool truncate)
    : target_table_(table), truncate(truncate) {
  LOG_TRACE("Creating a Delete Plan");
}

// Initializes the delete plan and retrieves coloum ids.
void DeletePlan::Init(parser::DeleteStatement *delete_statemenet, std::vector<oid_t>& columns) {
  LOG_TRACE("Creating a Delete Plan");
  table_name_ = delete_statemenet->GetTableName();
  auto database_name = delete_statemenet->GetDatabaseName();
  // Get target table based on database name and table name
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      database_name, table_name_);
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
  columns = {};
}

// Creates the update plan with sequential scan.
DeletePlan::DeletePlan(parser::DeleteStatement *delete_statemenet) {

  std::vector<oid_t> columns;
  Init(delete_statemenet, columns);
  LOG_TRACE("Creating a sequential scan plan");
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table_, expr_, columns));
  LOG_TRACE("Sequential scan plan created");
  AddChild(std::move(seq_scan_node));
}

// Creates the update plan with index scan.
DeletePlan::DeletePlan(parser::DeleteStatement *delete_statemenet,
                        std::vector<oid_t> &key_column_ids,
                        std::vector<ExpressionType> &expr_types,
                        std::vector<common::Value *> &values,
                        int &index_id) {
  std::vector<oid_t> columns;
  Init(delete_statemenet, columns);
  // Create index scan desc
  std::vector<expression::AbstractExpression*> runtime_keys;
  auto index = target_table_->GetIndex(index_id);
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);
    // Create plan node.
  LOG_TRACE("Creating a index scan plan");  
  std::unique_ptr<planner::IndexScanPlan> index_scan_node(
      new planner::IndexScanPlan(target_table_, expr_,
                                 columns, index_scan_desc, true));
  LOG_TRACE("Index scan plan created");
  AddChild(std::move(index_scan_node));
}

void DeletePlan::SetParameterValues(std::vector<common::Value *> *values) {
  LOG_TRACE("Setting parameter values in Delete");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

}  // namespace planner
}  // namespace peloton
