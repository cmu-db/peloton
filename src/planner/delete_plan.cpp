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

#include "parser/delete_statement.h"
#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"

#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/seq_scan_plan.h"

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

// Initializes the delete plan.
void DeletePlan::BuildInitialDeletePlan(
    parser::DeleteStatement *delete_statemenet,
    concurrency::Transaction *consistentTxn) {
  LOG_TRACE("Creating a Delete Plan");
  table_name_ = delete_statemenet->GetTableName();
  auto database_name = delete_statemenet->GetDatabaseName();
  // Get target table based on database name and table name
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      database_name, table_name_, consistentTxn);
  // if expr is null , delete all tuples from table
  if (delete_statemenet->expr == nullptr) {
    LOG_TRACE("No expression, setting truncate to true");
    expr_ = nullptr;
    truncate = true;

  } else {
    expr_ = delete_statemenet->expr->Copy();
    LOG_TRACE("Replacing COLUMN_REF with TupleValueExpressions");
    expression::ExpressionUtil::TransformExpression(target_table_->GetSchema(),
                                                    expr_);
  }
}

// Creates the delete plan. The index plan should be added outside
DeletePlan::DeletePlan(storage::DataTable *table,
                       const expression::AbstractExpression *predicate) {
  target_table_ = table;

  // if expr is null , delete all tuples from table
  if (predicate == nullptr) {
    LOG_TRACE("No expression, setting truncate to true");
    expr_ = nullptr;
    truncate = true;
  } else {
    expr_ = predicate->Copy();
    LOG_TRACE("Replacing COLUMN_REF with TupleValueExpressions");
    expression::ExpressionUtil::TransformExpression(target_table_->GetSchema(),
                                                    expr_);
  }
}

void DeletePlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Delete");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

}  // namespace planner
}  // namespace peloton
