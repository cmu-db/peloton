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
#include "expression/expression_util.h"

namespace peloton {

namespace expression{
class TupleValueExpression;
}
namespace planner {

DeletePlan::DeletePlan(storage::DataTable *table, bool truncate)
      : target_table_(table), truncate(truncate) {}

DeletePlan::DeletePlan(parser::DeleteStatement *delete_statemenet) {
  // Add your stuff here
  table_name = std::string(delete_statemenet->table_name);
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME, table_name);
  // if expr is null , delete all tuples from table
  if(delete_statemenet->expr == nullptr) {
	  LOG_INFO("No expression, setting truncate to true");
	  expr = nullptr;
	  truncate = true;

  }
  else {
	  expr = delete_statemenet->expr->Copy();
	  LOG_INFO("Replacing COLUMN_REF with TupleValueExpressions");
	  ReplaceColumnExpressions(target_table_->GetSchema(), expr);
  }
  std::vector<oid_t> column_ids = {};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table_, expr, column_ids));
  AddChild(std::move(seq_scan_node));
}

/**
 * This function replaces all COLUMN_REF expressions with TupleValue expressions
 */
void DeletePlan::ReplaceColumnExpressions(catalog::Schema *schema, expression::AbstractExpression* expression) {
  LOG_INFO("Expression Type --> %s", ExpressionTypeToString(expression->GetExpressionType()).c_str());
  LOG_INFO("Left Type --> %s", ExpressionTypeToString(expression->GetLeft()->GetExpressionType()).c_str());
  LOG_INFO("Right Type --> %s", ExpressionTypeToString(expression->GetRight()->GetExpressionType()).c_str());
  if(expression->GetLeft()->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF) {
    auto expr = expression->GetLeft();
    std::string col_name(expr->getName());
    LOG_INFO("Column name: %s", col_name.c_str());
    delete expr;
    expression->setLeft(expression::ExpressionUtil::ConvertToTupleValueExpression(schema, col_name));
  }
  else if (expression->GetRight()->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF) {
    auto expr = expression->GetRight();
    std::string col_name(expr->getName());
    LOG_INFO("Column name: %s", col_name.c_str());
    delete expr;
    expression->setRight(expression::ExpressionUtil::ConvertToTupleValueExpression(schema, col_name));
  }
  else {
	  ReplaceColumnExpressions(schema, expression->GetModifiableLeft());
	  ReplaceColumnExpressions(schema, expression->GetModifiableRight());

  }
}

void DeletePlan::SetParameterValues(std::vector<Value>* values) {
	expression::ExpressionUtil::ConvertParameterExpressions(expr, values);
}


}  // namespace planner
}  // namespace peloton
