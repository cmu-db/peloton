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

#include "planner/update_plan.h"

#include "planner/project_info.h"
#include "common/types.h"

#include "storage/data_table.h"
#include "catalog/bootstrapper.h"
#include "parser/statement_update.h"
#include "parser/table_ref.h"

namespace peloton {
namespace planner {

UpdatePlan::UpdatePlan(storage::DataTable *table,
		std::unique_ptr<const planner::ProjectInfo> project_info) :
		target_table_(table), project_info_(std::move(project_info)), updates(
				NULL), where(NULL) {
}

UpdatePlan::UpdatePlan(parser::UpdateStatement *parse_tree) {
	auto t_ref = parse_tree->table;
	table_name = std::string(t_ref->name);
	target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(
			DEFAULT_DB_NAME, table_name);
	updates = parse_tree->updates;

	TargetList tlist;
	DirectMapList dmlist;
	oid_t col_id;
	auto schema = target_table_->GetSchema();

	std::vector<oid_t> columns;
	for (auto update : *updates) {

		// get oid_t of the column and push it to the vector;
		col_id = schema->GetColumnID(std::string(update->column));

		LOG_INFO("This is the column ID -------------------------> %d" , col_id);
		
		columns.push_back(col_id);
		tlist.emplace_back(col_id, update->value->Copy());
	}

	for (uint i = 0; i < schema->GetColumns().size(); i++) {
		if(schema->GetColumns()[i].column_name != schema->GetColumns()[col_id].column_name)
		dmlist.emplace_back(i,
				std::pair<oid_t, oid_t>(0, i));
	}

	std::unique_ptr<const planner::ProjectInfo> project_info(
			new planner::ProjectInfo(std::move(tlist), std::move(dmlist)));
	project_info_ = std::move(project_info);
	
	where = parse_tree->where->Copy();
	auto expr = where->Copy();

	ReplaceColumnExpressions(expr);
	// auto right_expr = (expression::AbstractExpression*)expr->GetRight();
	// auto left_expr = (expression::AbstractExpression*)expr->GetLeft();

	// std::unique_ptr<expression::AbstractExpression> unique_right_expr(std::move(right_expr));
	// std::unique_ptr<expression::AbstractExpression> unique_left_expr(std::move(left_expr));

	// LOG_INFO("right_expr TYPE =========-----------------> %s" ,ExpressionTypeToString(right_expr->GetExpressionType()).c_str());
	// LOG_INFO("left_expr TYPE =========-----------------> %s" ,ExpressionTypeToString(left_expr->GetExpressionType()).c_str());


	// auto predicate = new expression::ComparisonExpression<expression::CmpGt>(
 //      expr->GetExpressionType(), unique_left_expr.release(), unique_right_expr.release());

	std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
			new planner::SeqScanPlan(target_table_, expr, columns));
	AddChild(std::move(seq_scan_node));
}

void UpdatePlan::ReplaceColumnExpressions(expression::AbstractExpression* expression) {
  LOG_INFO("Expression Type --> %s", ExpressionTypeToString(expression->GetExpressionType()).c_str());
  LOG_INFO("Left Type --> %s", ExpressionTypeToString(expression->GetLeft()->GetExpressionType()).c_str());
  LOG_INFO("Right Type --> %s", ExpressionTypeToString(expression->GetRight()->GetExpressionType()).c_str());
  if(expression->GetLeft()->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF) {
    auto expr = expression->GetLeft();
    std::string col_name(expr->getName());
    LOG_INFO("Column name: %s", col_name.c_str());
    delete expr;
    expression->setLeft(ConvertToTupleValueExpression(col_name));
  }
  else if (expression->GetRight()->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF) {
    auto expr = expression->GetRight();
    std::string col_name(expr->getName());
    LOG_INFO("Column name: %s", col_name.c_str());
    delete expr;
    expression->setRight(ConvertToTupleValueExpression(col_name));
  }
  else {
	  ReplaceColumnExpressions(expression->GetModifiableLeft());
	  ReplaceColumnExpressions(expression->GetModifiableRight());

  }
}
/**
 * This function generates a TupleValue expression from the column name
 */
expression::AbstractExpression* UpdatePlan::ConvertToTupleValueExpression (std::string column_name) {
	auto schema = target_table_->GetSchema();
    auto column_id = schema->GetColumnID(column_name);
    LOG_INFO("Column id in table: %u", column_id);
    expression::TupleValueExpression *expr =
        new expression::TupleValueExpression(schema->GetType(column_id), 0, column_id);
	return expr;
}

}  // namespace planner
}  // namespace peloton