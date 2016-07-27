//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_optimizer.cpp
//
// Identification: src/optimizer/simple_optimizer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "optimizer/simple_optimizer.h"

#include "parser/abstract_parse.h"
#include "parser/insert_parse.h"

#include "planner/abstract_plan.h"
#include "planner/drop_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/update_plan.h"
#include "planner/aggregate_plan.h"
#include "parser/abstract_parse.h"
#include "parser/drop_parse.h"
#include "parser/create_parse.h"
#include "expression/parser_expression.h"
#include "expression/expression_util.h"
#include "parser/sql_statement.h"
#include "parser/statements.h"
#include "catalog/bootstrapper.h"

#include "common/logger.h"

#include <memory>

namespace peloton {
namespace planner {
class AbstractPlan;
}
namespace optimizer {

SimpleOptimizer::SimpleOptimizer() {
}
;

SimpleOptimizer::~SimpleOptimizer() {
}
;

std::shared_ptr<planner::AbstractPlan> SimpleOptimizer::BuildPelotonPlanTree(
    const std::unique_ptr<parser::SQLStatement>& parse_tree) {

  std::shared_ptr<planner::AbstractPlan> plan_tree;

  // Base Case
  if (parse_tree.get() == nullptr)
    return plan_tree;

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  // One to one Mapping
  auto parse_item_node_type = parse_tree->GetType();

  switch (parse_item_node_type) {
    case STATEMENT_TYPE_DROP: {
    	LOG_INFO("Adding Drop plan...");
      std::unique_ptr<planner::AbstractPlan> child_DropPlan(
          new planner::DropPlan((parser::DropStatement*) parse_tree.get()));
      child_plan = std::move(child_DropPlan);
    }
      break;

    case STATEMENT_TYPE_CREATE: {
    	LOG_INFO("Adding Create plan...");
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(
          new planner::CreatePlan((parser::CreateStatement*) parse_tree.get()));
      child_plan = std::move(child_CreatePlan);
    }
      break;

    case STATEMENT_TYPE_SELECT: {
    	LOG_INFO("Processing SELECT...");
    	auto select_stmt = (parser::SelectStatement*) parse_tree.get();
    	int index = 0;
    	auto agg_type = AGGREGATE_TYPE_PLAIN; // default aggregator
    	std::vector<oid_t> group_by_columns;
    	auto group_by = select_stmt->group_by;
    	expression::AbstractExpression* having = nullptr;
    	auto target_table = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME,
    	    				select_stmt->from_table->name);


    	// Preparing the group by columns
    	if(group_by != NULL){
    		LOG_INFO("Found GROUP BY");
			for(auto elem : *group_by->columns) {
				std::string col_name(elem->getName());
				auto column_id = target_table->GetSchema()->GetColumnID(col_name);
				group_by_columns.push_back(column_id);
			}
			// Having Expression needs to be prepared
			// Currently it's mostly ParserExpression
			// Needs to be prepared
			having = group_by->having;
    	}

    	// Check if there are any aggregate functions
    	bool func_flag = false;
    	for(auto expr : *select_stmt->getSelectList()) {
    		if(expr->GetExpressionType() == EXPRESSION_TYPE_FUNCTION_REF) {
    			LOG_INFO("Query has aggregate functions");
    			func_flag = true;
    			break;
    		}
    	}

    	// If there is no aggregate functions, just do a sequential scan
    	if(!func_flag && group_by_columns.size() == 0) {
    		LOG_INFO("No aggregate functions found.");
    		std::unique_ptr<planner::AbstractPlan> child_SelectPlan(
    		          new planner::SeqScanPlan((parser::SelectStatement*) parse_tree.get()));
    		child_plan = std::move(child_SelectPlan);
    	}
    	// Else, do aggregations on top of scan
    	else {
    		// Create sequential scan plan
    		LOG_INFO("Creating a sequential scan plan");
    		target_table = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME,
    				select_stmt->from_table->name);
			std::vector<oid_t> column_ids = {};
			std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
				  new planner::SeqScanPlan(target_table, select_stmt->where_clause, std::move(column_ids)));
			LOG_INFO("Sequential scan plan created");

			// Prepare aggregate plan
			std::vector<catalog::Column> output_schema_columns;
    		std::vector<planner::AggregatePlan::AggTerm> agg_terms;
			DirectMapList direct_map_list = {};
			oid_t new_col_id = 0;
			oid_t agg_id = 0;
			int col_cntr_id = 0;
			for(auto expr : *select_stmt->getSelectList()) {
				LOG_INFO("Expression %d type in Select: %s", index++, ExpressionTypeToString(expr->GetExpressionType()).c_str());
				// If an aggregate function is found
				if(expr->GetExpressionType() == EXPRESSION_TYPE_FUNCTION_REF) {
					auto func_expr = (expression::ParserExpression*) expr;
					LOG_INFO("Expression type in Function Expression: %s",
							ExpressionTypeToString(func_expr->expr->GetExpressionType()).c_str());
					LOG_INFO("Distinct flag: %d", func_expr->distinct);
					// Count a column expression
					if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF) {

						  LOG_INFO("Function name: %s", func_expr->getName());
						  LOG_INFO("Aggregate type: %s",
								  ExpressionTypeToString(ParserExpressionNameToExpressionType(func_expr->getName())).c_str());
						  planner::AggregatePlan::AggTerm agg_term(
							  ParserExpressionNameToExpressionType(func_expr->getName()),
							  expression::ExpressionUtil::ConvertToTupleValueExpression(target_table->GetSchema(),
									  func_expr->expr->getName()),
							  func_expr->distinct);
						  agg_terms.push_back(agg_term);

						  std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, agg_id);
						  std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair = std::make_pair(new_col_id, inner_pair);
						  direct_map_list.emplace_back(outer_pair);
						  LOG_INFO("Direct map list: (%d, (%d, %d))", outer_pair.first,
									outer_pair.second.first, outer_pair.second.second);

						  if(ParserExpressionNameToExpressionType(func_expr->getName()) == EXPRESSION_TYPE_AGGREGATE_AVG) {

							  auto column = catalog::Column(VALUE_TYPE_DOUBLE,
									  GetTypeSize(VALUE_TYPE_DOUBLE),
									  "COL_" + std::to_string(col_cntr_id++), // COL_A should be used only when there is no AS
									  true);

							  output_schema_columns.push_back(column);
						  }
						  else {
							  oid_t old_col_id = target_table->GetSchema()->GetColumnID(func_expr->expr->getName());
								auto table_column = target_table->GetSchema()->GetColumn(old_col_id);

								auto column = catalog::Column(table_column.GetType(),
										GetTypeSize(table_column.GetType()),
										"COL_" + std::to_string(col_cntr_id++), // COL_A should be used only when there is no AS
										true);

								output_schema_columns.push_back(column);
						  }


					}
					// Count star
					else if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_STAR) {
						LOG_INFO("Creating an aggregate plan");
						planner::AggregatePlan::AggTerm agg_term(
							  EXPRESSION_TYPE_AGGREGATE_COUNT_STAR,
							  nullptr, // No predicate for star expression. Nothing to evaluate
							  func_expr->distinct);
						agg_terms.push_back(agg_term);

						std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, agg_id);
						std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair = std::make_pair(new_col_id, inner_pair);
						direct_map_list.emplace_back(outer_pair);
						LOG_INFO("Direct map list: (%d, (%d, %d))", outer_pair.first,
								outer_pair.second.first, outer_pair.second.second);

						auto column = catalog::Column(VALUE_TYPE_INTEGER,
					    		GetTypeSize(VALUE_TYPE_INTEGER),
								"COL_" + std::to_string(col_cntr_id++), // COL_A should be used only when there is no AS
								true);

					    output_schema_columns.push_back(column);
					}
					else {
						LOG_INFO("Unrecognized type in function expression!");
					}
					++agg_id;
				}
				// Column name
				else {
					agg_type = AGGREGATE_TYPE_HASH; // There are columns in the query
					std::string col_name(expr->getName());
					oid_t old_col_id = target_table->GetSchema()->GetColumnID(col_name);

				    std::pair<oid_t, oid_t> inner_pair = std::make_pair(0, old_col_id);
				    std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair = std::make_pair(new_col_id, inner_pair);
				    direct_map_list.emplace_back(outer_pair);
					LOG_INFO("Direct map list: (%d, (%d, %d))", outer_pair.first,
							outer_pair.second.first, outer_pair.second.second);

					auto table_column = target_table->GetSchema()->GetColumn(old_col_id);

					auto column = catalog::Column(table_column.GetType(),
							GetTypeSize(table_column.GetType()),
							"COL_" + std::to_string(col_cntr_id++), // COL_A should be used only when there is no AS
							true);

					output_schema_columns.push_back(target_table->GetSchema()->GetColumn(old_col_id));
				}
				++new_col_id;
			}

		  LOG_INFO("Creating a ProjectInfo");
		  std::unique_ptr<const planner::ProjectInfo> proj_info(
				  new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

		  std::unique_ptr<const expression::AbstractExpression> predicate(having);
		  std::shared_ptr<const catalog::Schema> output_table_schema(
		        new catalog::Schema(output_schema_columns));
		  LOG_INFO("Output Schema Info: %s", output_table_schema.get()->GetInfo().c_str());

	      std::unique_ptr<planner::AggregatePlan> child_agg_plan(new planner::AggregatePlan(std::move(proj_info),
	    		  std::move(predicate), std::move(agg_terms), std::move(group_by_columns),
	    	      output_table_schema, agg_type));

	      child_agg_plan->AddChild(std::move(seq_scan_node));
	      child_plan = std::move(child_agg_plan);

    	}

    }
    break;

    case STATEMENT_TYPE_INSERT: {
    	LOG_INFO("Adding Insert plan...");
      std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
          new planner::InsertPlan((parser::InsertStatement*) parse_tree.get()));
      child_plan = std::move(child_InsertPlan);
    }
      break;

    case STATEMENT_TYPE_DELETE: {
    	LOG_INFO("Adding Delete plan...");
          std::unique_ptr<planner::AbstractPlan> child_DeletePlan(
              new planner::DeletePlan((parser::DeleteStatement*) parse_tree.get()));
          child_plan = std::move(child_DeletePlan);
        }
     break;

    case STATEMENT_TYPE_UPDATE: {
    	LOG_INFO("Adding Update plan...");
          std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
              new planner::UpdatePlan((parser::UpdateStatement*) parse_tree.get()));
          child_plan = std::move(child_InsertPlan);
        }
          break;

    default:
      LOG_INFO("Unsupported Parse Node Type");
  }

  if (child_plan != nullptr) {
    if (plan_tree != nullptr) {
      plan_tree->AddChild(std::move(child_plan));
    }
    else {
      plan_tree = std::move(child_plan);
    }
  }

  // Recurse
  /*auto &children = parse_tree->GetChildren();
   for (auto &child : children) {
   std::shared_ptr<planner::AbstractPlan> child_parse = std::move(BuildPlanTree(child));
   child_plan = std::move(child_parse);
   }*/
  return plan_tree;
}

}  // namespace optimizer
}  // namespace peloton
