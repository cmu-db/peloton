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

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "expression/aggregate_expression.h"
#include "expression/expression_util.h"
#include "expression/function_expression.h"
#include "expression/star_expression.h"
#include "parser/sql_statement.h"
#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/aggregate_plan.h"
#include "planner/copy_plan.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/drop_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/limit_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/order_by_plan.h"
#include "planner/populate_index_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"
#include "catalog/query_metrics_catalog.h"

#include "common/logger.h"
#include "type/value_factory.h"

#include <memory>
#include <unordered_map>

namespace peloton {
namespace planner {
class AbstractPlan;
}
namespace optimizer {

SimpleOptimizer::SimpleOptimizer(){};

SimpleOptimizer::~SimpleOptimizer(){};

std::shared_ptr<planner::AbstractPlan> SimpleOptimizer::BuildPelotonPlanTree(
    const std::unique_ptr<parser::SQLStatementList>& parse_tree) {
  std::shared_ptr<planner::AbstractPlan> plan_tree;

  // Base Case
  if (parse_tree->GetStatements().size() == 0) return plan_tree;

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  // One to one Mapping
  auto stmt_type = parse_tree->GetStatements().at(0)->GetType();

  auto parse_tree2 = parse_tree->GetStatements().at(0);

  switch (stmt_type) {
    case StatementType::DROP: {
      LOG_TRACE("Adding Drop plan...");
      std::unique_ptr<planner::AbstractPlan> child_DropPlan(
          new planner::DropPlan((parser::DropStatement*)parse_tree2));
      child_plan = std::move(child_DropPlan);
    } break;

    case StatementType::CREATE: {
      LOG_TRACE("Adding Create plan...");

      auto create_plan =
          new planner::CreatePlan((parser::CreateStatement*)parse_tree2);
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(create_plan);
      child_plan = std::move(child_CreatePlan);
      // If creating index, populate with existing data first.
      if (create_plan->GetCreateType() == peloton::CreateType::INDEX) {
        auto create_stmt = (parser::CreateStatement*)parse_tree2;
        auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
            create_stmt->GetDatabaseName(), create_stmt->GetTableName());
        std::vector<oid_t> column_ids;
        auto schema = target_table->GetSchema();
        for (auto column_name : create_plan->GetIndexAttributes()) {
          column_ids.push_back(schema->GetColumnID(column_name));
        }
        // Create a plan to retrieve data
        std::unique_ptr<planner::AbstractPlan> child_SeqScanPlan =
            CreateScanPlan(target_table, column_ids, nullptr, false);
        child_SeqScanPlan->AddChild(std::move(child_plan));
        child_plan = std::move(child_SeqScanPlan);
        // Create a plan to add data to index
        std::unique_ptr<planner::AbstractPlan> child_PopulateIndexPlan(
            new planner::PopulateIndexPlan(target_table, column_ids));
        child_PopulateIndexPlan->AddChild(std::move(child_plan));
        child_plan = std::move(child_PopulateIndexPlan);
      }
    } break;

    case StatementType::SELECT: {
      LOG_TRACE("Processing SELECT...");
      auto select_stmt = (parser::SelectStatement*)parse_tree2;
      LOG_TRACE("SELECT Info: %s", select_stmt->GetInfo().c_str());
      auto agg_type = AggregateType::PLAIN;  // default aggregator
      std::vector<oid_t> group_by_columns;
      bool order_plan_added = false;
      auto group_by = select_stmt->group_by;
      expression::AbstractExpression* having = nullptr;

      // The HACK to make the join in tpcc work. This is written by Joy Arulraj
      if (select_stmt->from_table->list != NULL) {
        LOG_TRACE("have join condition? %d",
                  select_stmt->from_table->join != NULL);
        LOG_TRACE("have sub select statement? %d",
                  select_stmt->from_table->select != NULL);
        try {
          catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                            "order_line");
          // auto child_SelectPlan = CreateHackingJoinPlan();
          auto child_SelectPlan = CreateHackingNestedLoopJoinPlan(select_stmt);
          child_plan = std::move(child_SelectPlan);
          break;
        } catch (Exception& e) {
          throw NotImplementedException("Error: Joins are not implemented yet");
        }
      }

      if (select_stmt->from_table->join != NULL) {
        child_plan = CreateJoinPlan(select_stmt);
        break;
        // throw NotImplementedException("Error: Joins are not implemented
        // yet");
      }

      storage::DataTable* target_table =
          catalog::Catalog::GetInstance()->GetTableWithName(
              select_stmt->from_table->GetDatabaseName(),
              select_stmt->from_table->GetTableName());

      // Preparing the group by columns
      if (group_by != NULL) {
        LOG_TRACE("Found GROUP BY");
        for (auto elem : *group_by->columns) {
          auto tuple_elem = (expression::TupleValueExpression*)elem;
          std::string col_name(tuple_elem->GetColumnName());
          auto column_id = target_table->GetSchema()->GetColumnID(col_name);
          group_by_columns.push_back(column_id);
        }
        // Having Expression needs to be prepared
        // Currently it's mostly ParserExpression
        // Needs to be prepared
        having = group_by->having;
      }

      std::vector<oid_t> column_ids;
      auto& schema = *target_table->GetSchema();
      auto predicate = select_stmt->where_clause;
      bool needs_projection = false;
      expression::ExpressionUtil::TransformExpression(&schema, predicate);
      bool is_star = (*select_stmt->getSelectList())[0]->GetExpressionType() ==
                     ExpressionType::STAR;

      for (auto col : *select_stmt->select_list) {
        expression::ExpressionUtil::TransformExpression(column_ids, col, schema,
                                                        needs_projection);
      }

      // Adds order by column to column ids if it was not added through
      // select_list
      // No problem given that the underlying structure is a map.
      // If query is 'select *', the column will be already added.
      if (select_stmt->order != nullptr && !is_star) {
        expression::ExpressionUtil::TransformExpression(
            column_ids, select_stmt->order->exprs->at(0), schema,
            needs_projection);
      }

      // Check if there are any aggregate functions
      bool agg_flag = false;
      for (auto expr : *select_stmt->getSelectList()) {
        if (expression::ExpressionUtil::IsAggregateExpression(
                expr->GetExpressionType())) {
          LOG_TRACE("Query has aggregate functions");
          agg_flag = true;
          break;
        }
      }

      // If there is no aggregate functions, just do a sequential scan
      if (!agg_flag && group_by_columns.size() == 0) {
        LOG_TRACE("No aggregate functions found.");
        std::unique_ptr<planner::AbstractPlan> child_SelectPlan =
            CreateScanPlan(target_table, column_ids, predicate,
                           select_stmt->is_for_update);

        // if we have expressions which are not just columns, we need to add a
        // projection plan node
        if (needs_projection) {
          auto& select_list = *select_stmt->getSelectList();
          // expressions to evaluate
          TargetList tl = TargetList();
          // columns which can be returned directly
          DirectMapList dml = DirectMapList();
          // schema of the projections output
          std::vector<catalog::Column> columns;
          const catalog::Schema* table_schema = target_table->GetSchema();
          for (int i = 0; i < (int)select_list.size(); i++) {
            auto expr = select_list[i];
            // if the root of the expression is a column value we can
            // just do a direct mapping
            if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
              auto tup_expr = (expression::TupleValueExpression*)expr;
              oid_t old_col_id =
                  table_schema->GetColumnID(tup_expr->GetColumnName());
              columns.push_back(table_schema->GetColumn(old_col_id));
              dml.push_back(
                  DirectMap(i, std::make_pair(0, tup_expr->GetColumnId())));
            }
            // otherwise we need to evaluate the expression
            else {
              planner::DerivedAttribute attribute;
              attribute.expr = expr->Copy();
              attribute.attribute_info.type = attribute.expr->GetValueType();
              tl.push_back(Target(i, attribute));
              columns.push_back(
                  catalog::Column(expr->GetValueType(),
                                  type::Type::GetTypeSize(expr->GetValueType()),
                                  "expr" + std::to_string(i)));
            }
          }
          if (select_stmt->order != NULL) {
            if (select_stmt->limit != NULL) {
              auto order_limit_plan = CreateOrderByLimitPlan(
                  select_stmt, child_SelectPlan.get(),
                  target_table->GetSchema(), column_ids, is_star);
              order_limit_plan->GetChildren()[0]->AddChild(
                  std::move(child_SelectPlan));
              child_SelectPlan = std::move(order_limit_plan);
            } else {
              auto order_plan = CreateOrderByPlan(
                  select_stmt, child_SelectPlan.get(),
                  target_table->GetSchema(), column_ids, is_star);
              order_plan->AddChild(std::move(child_SelectPlan));
              child_SelectPlan = std::move(order_plan);
            }
            order_plan_added = true;
          }

          // build the projection plan node and insert aboce the scan
          std::unique_ptr<planner::ProjectInfo> proj_info(
              new planner::ProjectInfo(std::move(tl), std::move(dml)));
          std::shared_ptr<catalog::Schema> schema_ptr(
              new catalog::Schema(columns));
          std::unique_ptr<planner::AbstractPlan> child_ProjectPlan(
              new planner::ProjectionPlan(std::move(proj_info), schema_ptr));
          child_ProjectPlan->AddChild(std::move(child_SelectPlan));
          child_SelectPlan = std::move(child_ProjectPlan);
        }

        // Workflow for DISTINCT
        if (select_stmt->select_distinct) {
          std::vector<std::unique_ptr<const expression::AbstractExpression>>
              hash_keys;
          if (is_star) {
            for (auto col : target_table->GetSchema()->GetColumns()) {
              auto key =
                  expression::ExpressionUtil::ConvertToTupleValueExpression(
                      target_table->GetSchema(), col.GetName());
              hash_keys.emplace_back(key);
            }
          } else {
            for (auto col : *select_stmt->select_list) {
              // Copy column for handling of unique_ptr
              auto copy_col = col->Copy();
              hash_keys.emplace_back(copy_col);
            }
          }
          // Create hash plan node
          std::unique_ptr<planner::HashPlan> hash_plan_node(
              new planner::HashPlan(hash_keys));
          hash_plan_node->AddChild(std::move(child_SelectPlan));
          child_SelectPlan = std::move(hash_plan_node);
        }
        // Workflow for ORDER_BY and LIMIT
        if (select_stmt->order != NULL && select_stmt->limit != NULL &&
            !order_plan_added) {
          auto order_limit_plan = CreateOrderByLimitPlan(
              select_stmt, child_SelectPlan.get(), target_table->GetSchema(),
              column_ids, is_star);
          order_limit_plan->GetChildren()[0]->AddChild(
              std::move(child_SelectPlan));
          child_plan = std::move(order_limit_plan);
        }
        // Only order by statement (without limit)
        else if (select_stmt->order != NULL && !order_plan_added) {
          auto order_plan =
              CreateOrderByPlan(select_stmt, child_SelectPlan.get(),
                                target_table->GetSchema(), column_ids, is_star);
          order_plan->AddChild(std::move(child_SelectPlan));
          child_plan = std::move(order_plan);
        }
        // limit statement without order by
        else if (select_stmt->limit != NULL) {
          int offset = select_stmt->limit->offset;
          if (offset < 0) {
            offset = 0;
          }

          // Set the flag for the underlying index scan plan to accelerate the
          // limit operation speed. That is to say the limit flags are passed
          // to index, then index returns the tuples matched with the limit
          SetIndexScanFlag(child_SelectPlan.get(), select_stmt->limit->limit,
                           offset);

          // Create limit_plan
          std::unique_ptr<planner::LimitPlan> limit_plan(
              new planner::LimitPlan(select_stmt->limit->limit, offset));
          limit_plan->AddChild(std::move(child_SelectPlan));
          child_plan = std::move(limit_plan);
        } else {
          child_plan = std::move(child_SelectPlan);
        }
      }
      // Else, do aggregations on top of scan
      else {
        std::unique_ptr<planner::AbstractScan> scan_node = CreateScanPlan(
            target_table, column_ids, predicate, select_stmt->is_for_update);

        // Prepare aggregate plan
        std::vector<catalog::Column> output_schema_columns;
        std::vector<planner::AggregatePlan::AggTerm> agg_terms;
        DirectMapList direct_map_list = {};
        oid_t new_col_id = 0;
        oid_t agg_id = 0;
        int col_cntr_id = 0;
        for (auto expr : *select_stmt->getSelectList()) {
          LOG_TRACE("Expression type in Select: %s",
                    ExpressionTypeToString(expr->GetExpressionType()).c_str());

          // If an aggregate function is found
          if (expression::ExpressionUtil::IsAggregateExpression(
                  expr->GetExpressionType())) {
            auto agg_expr = (expression::AggregateExpression*)expr;
            LOG_TRACE(
                "Expression type in Function Expression: %s",
                ExpressionTypeToString(expr->GetExpressionType()).c_str());
            LOG_TRACE("Distinct flag: %d", agg_expr->distinct_);

            // Count a column expression
            if (agg_expr->GetChild(0) != nullptr &&
                agg_expr->GetChild(0)->GetExpressionType() ==
                    ExpressionType::VALUE_TUPLE) {
              auto agg_over =
                  (expression::TupleValueExpression*)agg_expr->GetChild(0);
              LOG_TRACE("Function name: %s",
                        ((expression::TupleValueExpression*)agg_expr)
                            ->GetExpressionName());
              LOG_TRACE("Aggregate type: %s",
                        ExpressionTypeToString(
                            ParserExpressionNameToExpressionType(
                                expr->GetExpressionName())).c_str());
              planner::AggregatePlan::AggTerm agg_term(
                  agg_expr->GetExpressionType(), agg_over->Copy(),
                  agg_expr->distinct_);
              agg_terms.push_back(agg_term);

              std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, agg_id);
              std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
                  std::make_pair(new_col_id, inner_pair);
              direct_map_list.emplace_back(outer_pair);
              LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
                        outer_pair.second.first, outer_pair.second.second);

              // If aggregate type is average the value type should be double
              if (agg_expr->GetExpressionType() ==
                  ExpressionType::AGGREGATE_AVG) {
                // COL_A should be used only when there is no AS
                auto column = catalog::Column(
                    type::Type::DECIMAL,
                    type::Type::GetTypeSize(type::Type::DECIMAL),
                    "COL_" + std::to_string(col_cntr_id++), true);

                output_schema_columns.push_back(column);
              }

              // Else it is the same as the column type
              else {
                oid_t old_col_id = target_table->GetSchema()->GetColumnID(
                    agg_over->GetColumnName());
                auto table_column =
                    target_table->GetSchema()->GetColumn(old_col_id);

                auto column = catalog::Column(
                    table_column.GetType(),
                    type::Type::GetTypeSize(table_column.GetType()),
                    "COL_" + std::to_string(col_cntr_id++),  // COL_A should be
                                                             // used only when
                                                             // there is no AS
                    false);

                output_schema_columns.push_back(column);
              }

            }

            // Check for COUNT STAR Expression
            else if (agg_expr->GetExpressionType() ==
                     ExpressionType::AGGREGATE_COUNT_STAR) {
              LOG_TRACE("Creating an aggregate plan");
              planner::AggregatePlan::AggTerm agg_term(
                  ExpressionType::AGGREGATE_COUNT_STAR,
                  nullptr,  // No predicate for star expression. Nothing to
                            // evaluate
                  agg_expr->distinct_);
              agg_terms.push_back(agg_term);

              std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, agg_id);
              std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
                  std::make_pair(new_col_id, inner_pair);
              direct_map_list.emplace_back(outer_pair);
              LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
                        outer_pair.second.first, outer_pair.second.second);

              auto column = catalog::Column(
                  type::Type::INTEGER,
                  type::Type::GetTypeSize(type::Type::INTEGER),
                  "COL_" + std::to_string(col_cntr_id++),  // COL_A should be
                                                           // used only when
                                                           // there is no AS
                  true);

              output_schema_columns.push_back(column);
            } else {
              LOG_TRACE("Unrecognized type in function expression!");
              throw PlannerException(
                  "Error: Unrecognized type in function expression");
            }
            ++agg_id;
          }

          // Column name
          else {
            // There are columns in the query
            agg_type = AggregateType::HASH;
            std::string col_name(expr->GetExpressionName());
            oid_t old_col_id = target_table->GetSchema()->GetColumnID(col_name);

            std::pair<oid_t, oid_t> inner_pair = std::make_pair(0, old_col_id);
            std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
                std::make_pair(new_col_id, inner_pair);
            direct_map_list.emplace_back(outer_pair);
            LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
                      outer_pair.second.first, outer_pair.second.second);

            auto table_column =
                target_table->GetSchema()->GetColumn(old_col_id);

            auto column = catalog::Column(
                table_column.GetType(),
                type::Type::GetTypeSize(table_column.GetType()),
                "COL_" + std::to_string(col_cntr_id++),  // COL_A should be used
                                                         // only when there is
                                                         // no AS
                true);

            output_schema_columns.push_back(
                target_table->GetSchema()->GetColumn(old_col_id));
          }
          ++new_col_id;
        }
        LOG_TRACE("Creating a ProjectInfo");
        std::unique_ptr<const planner::ProjectInfo> proj_info(
            new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

        std::unique_ptr<const expression::AbstractExpression> predicate(having);
        std::shared_ptr<const catalog::Schema> output_table_schema(
            new catalog::Schema(output_schema_columns));
        LOG_TRACE("Output Schema Info: %s",
                  output_table_schema.get()->GetInfo().c_str());

        std::unique_ptr<planner::AggregatePlan> child_agg_plan(
            new planner::AggregatePlan(
                std::move(proj_info), std::move(predicate),
                std::move(agg_terms), std::move(group_by_columns),
                output_table_schema, agg_type));

        child_agg_plan->AddChild(std::move(scan_node));
        child_plan = std::move(child_agg_plan);
      }

    } break;

    case StatementType::INSERT: {
      LOG_TRACE("Adding Insert plan...");
      parser::InsertStatement* insertStmt =
          (parser::InsertStatement*)parse_tree2;

      storage::DataTable* target_table =
          catalog::Catalog::GetInstance()->GetTableWithName(
              insertStmt->GetDatabaseName(), insertStmt->GetTableName());

      std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
          new planner::InsertPlan(target_table, insertStmt->columns,
                                  insertStmt->insert_values));

      child_plan = std::move(child_InsertPlan);
    } break;

    case StatementType::COPY: {
      LOG_TRACE("Adding Copy plan...");
      parser::CopyStatement* copy_parse_tree =
          static_cast<parser::CopyStatement*>(parse_tree2);
      child_plan = std::move(CreateCopyPlan(copy_parse_tree));
    } break;

    case StatementType::DELETE: {
      LOG_TRACE("Adding Delete plan...");

      // column predicates passing to the index
      std::vector<oid_t> key_column_ids;
      std::vector<ExpressionType> expr_types;
      std::vector<type::Value> values;
      oid_t index_id;

      parser::DeleteStatement* deleteStmt =
          (parser::DeleteStatement*)parse_tree2;
      auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
          deleteStmt->GetDatabaseName(), deleteStmt->GetTableName());
      if (CheckIndexSearchable(target_table, deleteStmt->expr, key_column_ids,
                               expr_types, values, index_id)) {
        // Remove redundant predicate that index can search
        auto original_predicate_cpy = deleteStmt->expr->Copy();
        auto tailered_predicate =
            expression::ExpressionUtil::RemoveTermsWithIndexedColumns(
                original_predicate_cpy, target_table->GetIndex(index_id));
        if (tailered_predicate != original_predicate_cpy)
          delete original_predicate_cpy;

        // Create delete plan
        std::unique_ptr<planner::DeletePlan> child_DeletePlan(
            new planner::DeletePlan(target_table, tailered_predicate));

        // Create index scan plan
        std::vector<oid_t> columns;
        std::vector<expression::AbstractExpression*> runtime_keys;
        auto index = target_table->GetIndex(index_id);
        planner::IndexScanPlan::IndexScanDesc index_scan_desc(
            index, key_column_ids, expr_types, values, runtime_keys);
        LOG_TRACE("Creating a index scan plan");

        std::unique_ptr<planner::IndexScanPlan> index_scan_node(
            new planner::IndexScanPlan(target_table, tailered_predicate,
                                       columns, index_scan_desc, true));
        LOG_TRACE("Index scan plan created");

        // Add index scan plan
        child_DeletePlan->AddChild(std::move(index_scan_node));

        // Finish
        child_plan = std::move(child_DeletePlan);
      } else {
        // Create delete plan
        std::unique_ptr<planner::DeletePlan> child_DeletePlan(
            new planner::DeletePlan(target_table, deleteStmt->expr));

        // Create sequential scan plan
        expression::AbstractExpression* scan_expr =
            (child_DeletePlan->GetPredicate() == nullptr
                 ? nullptr
                 : child_DeletePlan->GetPredicate()->Copy());
        LOG_TRACE("Creating a sequential scan plan");
        std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
            new planner::SeqScanPlan(target_table, scan_expr, {}));
        LOG_TRACE("Sequential scan plan created");

        // Add seq scan plan
        child_DeletePlan->AddChild(std::move(seq_scan_node));

        // Finish
        child_plan = std::move(child_DeletePlan);
      }
    } break;

    case StatementType::UPDATE: {
      LOG_TRACE("Adding Update plan...");

      // column predicates passing to the index
      std::vector<oid_t> key_column_ids;
      std::vector<ExpressionType> expr_types;
      std::vector<type::Value> values;
      oid_t index_id;

      parser::UpdateStatement* updateStmt =
          (parser::UpdateStatement*)parse_tree2;
      auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
          updateStmt->table->GetDatabaseName(),
          updateStmt->table->GetTableName());

      auto update_clauses = updateStmt->updates;

      for (auto clause : *update_clauses) {
        auto clause_expr = clause->value;
        auto columnID = target_table->GetSchema()->GetColumnID(clause->column);
        auto column = target_table->GetSchema()->GetColumn(columnID);

        if (clause_expr->GetExpressionType() ==
            ExpressionType::VALUE_CONSTANT) {
          auto value = static_cast<const expression::ConstantValueExpression*>(
                           clause_expr)
                           ->GetValue()
                           .CastAs(column.GetType());
          auto value_expression =
              new expression::ConstantValueExpression(value);

          delete clause->value;
          clause->value = value_expression;

        } else {
          for (unsigned int child_index = 0;
               child_index < clause_expr->GetChildrenSize(); child_index++) {
            auto child = clause_expr->GetChild(child_index);

            if (child->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
              auto value =
                  static_cast<const expression::ConstantValueExpression*>(child)
                      ->GetValue()
                      .CastAs(column.GetType());
              auto value_expression =
                  new expression::ConstantValueExpression(value);

              clause_expr->SetChild(child_index, value_expression);
            }
          }
        }
      }

      if (CheckIndexSearchable(target_table, updateStmt->where, key_column_ids,
                               expr_types, values, index_id)) {
        // Remove redundant predicate that index can search
        // TODO: This is a HACK... Because of the gross way the update plan
        // constructor is written, I have to temporarily store the pointer of
        // the where clause in the update statement and then copy is back. This
        // will be gone after we refactor the UpdatePlan to the correct way.
        auto old_predicate = updateStmt->where;

        // Get predicate that removes all the index columns
        auto original_predicate_cpy = updateStmt->where->Copy();
        auto tailered_predicate =
            expression::ExpressionUtil::RemoveTermsWithIndexedColumns(
                original_predicate_cpy, target_table->GetIndex(index_id));
        if (tailered_predicate != original_predicate_cpy)
          delete original_predicate_cpy;

        updateStmt->where = tailered_predicate;

        // Create index scan plan
        std::unique_ptr<planner::AbstractPlan> child_UpdatePlan(
            new planner::UpdatePlan(updateStmt, key_column_ids, expr_types,
                                    values, index_id));
        // Within UpatePlan, it will copy the predicate. This is ugly since now
        // some plans copy the predicate and some don't. We need to unify them
        // someday
        delete tailered_predicate;
        updateStmt->where = old_predicate;

        child_plan = std::move(child_UpdatePlan);

      } else {
        // Create sequential scan plan
        std::unique_ptr<planner::AbstractPlan> child_UpdatePlan(
            new planner::UpdatePlan(updateStmt));
        child_plan = std::move(child_UpdatePlan);
      }
    } break;

    case StatementType::TRANSACTION: {
      // Nothing???
      break;
    }
    default:
      LOG_ERROR("Unsupported StatementType %s",
                StatementTypeToString(stmt_type).c_str());
      throw NotImplementedException("Error: Query plan not implemented");
  }

  if (child_plan != nullptr) {
    if (plan_tree != nullptr) {
      plan_tree->AddChild(std::move(child_plan));
    } else {
      plan_tree = std::move(child_plan);
    }
  }

  // Recurse will be modified later
  /*auto &children = parse_tree->GetChildren();
   for (auto &child : children) {
   std::shared_ptr<planner::AbstractPlan> child_parse =
   std::move(BuildPlanTree(child));
   child_plan = std::move(child_parse);
   }*/
  return plan_tree;
}

std::unique_ptr<planner::AbstractPlan> SimpleOptimizer::CreateCopyPlan(
    parser::CopyStatement* copy_stmt) {
  std::string table_name(copy_stmt->cpy_table->GetTableName());
  bool deserialize_parameters = false;

  // If we're copying the query metric table, then we need to handle the
  // deserialization of prepared stmt parameters
  if (table_name == QUERY_METRICS_CATALOG_NAME) {
    LOG_DEBUG("Copying the query_metric table.");
    deserialize_parameters = true;
  }

  std::unique_ptr<planner::AbstractPlan> copy_plan(
      new planner::CopyPlan(copy_stmt->file_path, deserialize_parameters));

  // Next, generate a dummy select * plan for target table
  // Hard code star expression
  auto star_expr = new peloton::expression::StarExpression();

  // Push star expression to list
  auto select_list =
      new std::vector<peloton::expression::AbstractExpression*>();
  select_list->push_back(star_expr);

  // Construct select stmt
  std::unique_ptr<parser::SelectStatement> select_stmt(
      new parser::SelectStatement());
  select_stmt->from_table = copy_stmt->cpy_table;
  copy_stmt->cpy_table = nullptr;
  select_stmt->select_list = select_list;

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      select_stmt->from_table->GetDatabaseName(), table_name);

  std::vector<oid_t> column_ids;
  bool needs_projection = false;
  expression::ExpressionUtil::TransformExpression(
      column_ids, nullptr, *target_table->GetSchema(), needs_projection);

  auto select_plan =
      SimpleOptimizer::CreateScanPlan(target_table, column_ids, nullptr, false);
  LOG_DEBUG("Sequential scan plan for copy created");

  // Attach it to the copy plan
  copy_plan->AddChild(std::move(select_plan));
  return std::move(copy_plan);
}

/**
 * This function checks whether the current expression can enable index
 * scan for the statement. If it is index searchable, returns true and
 * set the corresponding data structures that will be used in creating
 * index scan node. Otherwise, returns false.
 */
bool SimpleOptimizer::CheckIndexSearchable(
    storage::DataTable* target_table,
    expression::AbstractExpression* expression,
    std::vector<oid_t>& key_column_ids, std::vector<ExpressionType>& expr_types,
    std::vector<type::Value>& values, oid_t& index_id) {
  bool index_searchable = false;
  index_id = 0;

  // column predicates between the tuple value and the constant in the where
  // clause
  std::vector<oid_t> predicate_column_ids = {};
  std::vector<ExpressionType> predicate_expr_types;
  std::vector<type::Value> predicate_values;

  if (expression != NULL) {
    index_searchable = true;

    LOG_TRACE("Getting predicate columns");
    GetPredicateColumns(target_table->GetSchema(), expression,
                        predicate_column_ids, predicate_expr_types,
                        predicate_values, index_searchable);
    LOG_TRACE("Finished Getting predicate columns");

    if (index_searchable == true) {
      index_searchable = false;

      // Loop through the indexes to find to most proper one (if any)
      int index_index = 0;
      for (auto& column_set : target_table->GetIndexColumns()) {
        int matched_columns = 0;
        for (auto column_id : predicate_column_ids)
          if (column_set.find(column_id) != column_set.end()) matched_columns++;
        if (matched_columns == (int)column_set.size()) {
          index_searchable = true;
          index_id = index_index;
        }
        index_index++;
      }
    }
  }

  if (!index_searchable) {
    LOG_DEBUG("No suitable index for table '%s' exists. Skipping...",
              target_table->GetName().c_str());
    return (false);
  }

  // Prepares arguments for the index scan plan
  auto index = target_table->GetIndex(index_id);
  if (index == nullptr) return false;

  // Check whether the index is visible
  // This is for the IndexTuner demo
  if (index->GetMetadata()->GetVisibility() == false) {
    LOG_DEBUG("Index '%s.%s' is not visible. Skipping...",
              target_table->GetName().c_str(), index->GetName().c_str());
    return (false);
  }

  auto index_columns = target_table->GetIndexColumns()[index_id];
  int column_idx = 0;
  for (auto column_id : predicate_column_ids) {
    if (index_columns.find(column_id) != index_columns.end()) {
      key_column_ids.push_back(column_id);
      expr_types.push_back(predicate_expr_types[column_idx]);
      values.push_back(predicate_values[column_idx]);
      LOG_TRACE("Adding for IndexScanDesc: id(%d), expr(%s), values(%s)",
                column_id, ExpressionTypeToString(*expr_types.rbegin()).c_str(),
                (*values.rbegin()).GetInfo().c_str());
    }
    column_idx++;
  }

  return true;
}

std::unique_ptr<planner::AbstractScan> SimpleOptimizer::CreateScanPlan(
    storage::DataTable* target_table, std::vector<oid_t>& column_ids,
    expression::AbstractExpression* predicate, bool for_update) {
  oid_t index_id = 0;

  // column predicates passing to the index
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;

  if (!CheckIndexSearchable(target_table, predicate, key_column_ids, expr_types,
                            values, index_id)) {
    // Create sequential scan plan
    LOG_TRACE("Creating a sequential scan plan");
    auto predicate_cpy = predicate == nullptr ? nullptr : predicate->Copy();
    std::unique_ptr<planner::SeqScanPlan> child_SelectPlan(
        new planner::SeqScanPlan(target_table, predicate_cpy, column_ids,
                                 for_update));
    LOG_TRACE("Sequential scan plan created");
    return std::move(child_SelectPlan);
  }

  LOG_TRACE("predicate before remove: %s", predicate->GetInfo().c_str());
  // Remove redundant predicate that index can search
  auto index = target_table->GetIndex(index_id);
  auto original_predicate_cpy = predicate->Copy();
  auto tailered_predicate =
      expression::ExpressionUtil::RemoveTermsWithIndexedColumns(
          original_predicate_cpy, index);
  if (tailered_predicate != original_predicate_cpy)
    delete original_predicate_cpy;

  // Create index scan plan
  LOG_TRACE("Creating a index scan plan");
  std::vector<expression::AbstractExpression*> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  std::unique_ptr<planner::IndexScanPlan> node(
      new planner::IndexScanPlan(target_table, tailered_predicate, column_ids,
                                 index_scan_desc, for_update));
  LOG_TRACE("Index scan plan created");

  return std::move(node);
}

/**
 * This function replaces all COLUMN_REF expressions with TupleValue
 * expressions
 */
void SimpleOptimizer::GetPredicateColumns(
    const catalog::Schema* schema, expression::AbstractExpression* expression,
    std::vector<oid_t>& column_ids, std::vector<ExpressionType>& expr_types,
    std::vector<type::Value>& values, bool& index_searchable) {
  // For now, all conjunctions should be AND when using index scan.
  if (expression->GetExpressionType() == ExpressionType::CONJUNCTION_OR)
    index_searchable = false;

  LOG_TRACE("Expression Type --> %s",
            ExpressionTypeToString(expression->GetExpressionType()).c_str());
  if (!(expression->GetChild(0) && expression->GetChild(1))) return;
  LOG_TRACE("Left Type --> %s",
            ExpressionTypeToString(expression->GetChild(0)->GetExpressionType())
                .c_str());
  LOG_TRACE("Right Type --> %s",
            ExpressionTypeToString(expression->GetChild(1)->GetExpressionType())
                .c_str());

  // We're only supporting comparing a column_ref to a constant/parameter for
  // index scan right now
  if (expression->GetChild(0)->GetExpressionType() ==
      ExpressionType::VALUE_TUPLE) {
    auto right_type = expression->GetChild(1)->GetExpressionType();
    if (right_type == ExpressionType::VALUE_CONSTANT ||
        right_type == ExpressionType::VALUE_PARAMETER) {
      auto expr = (expression::TupleValueExpression*)expression->GetChild(0);
      std::string col_name(expr->GetColumnName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      auto column_id = schema->GetColumnID(col_name);
      column_ids.push_back(column_id);
      expr_types.push_back(expression->GetExpressionType());

      // Potential memory leak in line 664:
      // 24 bytes in 1 blocks are definitely lost in loss record 1 of 3
      // malloc (in /usr/lib/valgrind/vgpreload_memcheck-amd64-linux.so)
      // peloton::do_allocation(unsigned long, bool) (allocator.cpp:27)
      // operator new(unsigned long) (allocator.cpp:40)
      // peloton::type::IntegerValue::Copy() const (numeric_value.cpp:1288)
      // peloton::expression::ConstantValueExpression::GetValue() const
      // (constant_value_expression.h:40)
      if (right_type == ExpressionType::VALUE_CONSTANT) {
        values.push_back(reinterpret_cast<expression::ConstantValueExpression*>(
                             expression->GetModifiableChild(1))->GetValue());
        LOG_TRACE("Value Type: %d",
                  reinterpret_cast<expression::ConstantValueExpression*>(
                      expression->GetModifiableChild(1))->GetValueType());
      } else
        values.push_back(
            type::ValueFactory::GetParameterOffsetValue(
                reinterpret_cast<expression::ParameterValueExpression*>(
                    expression->GetModifiableChild(1))->GetValueIdx()).Copy());
      LOG_TRACE("Parameter offset: %s", (*values.rbegin()).GetInfo().c_str());
    }
  } else if (expression->GetChild(1)->GetExpressionType() ==
             ExpressionType::VALUE_TUPLE) {
    auto left_type = expression->GetChild(0)->GetExpressionType();
    if (left_type == ExpressionType::VALUE_CONSTANT ||
        left_type == ExpressionType::VALUE_PARAMETER) {
      auto expr = (expression::TupleValueExpression*)expression->GetChild(1);
      std::string col_name(expr->GetColumnName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      auto column_id = schema->GetColumnID(col_name);
      LOG_TRACE("Column id: %d", column_id);
      column_ids.push_back(column_id);
      expr_types.push_back(expression->GetExpressionType());

      if (left_type == ExpressionType::VALUE_CONSTANT) {
        values.push_back(reinterpret_cast<expression::ConstantValueExpression*>(
                             expression->GetModifiableChild(1))->GetValue());
        LOG_TRACE("Value Type: %d",
                  reinterpret_cast<expression::ConstantValueExpression*>(
                      expression->GetModifiableChild(0))->GetValueType());
      } else
        values.push_back(
            type::ValueFactory::GetParameterOffsetValue(
                reinterpret_cast<expression::ParameterValueExpression*>(
                    expression->GetModifiableChild(0))->GetValueIdx()).Copy());
      LOG_TRACE("Parameter offset: %s", (*values.rbegin()).GetInfo().c_str());
    }
  } else {
    GetPredicateColumns(schema, expression->GetModifiableChild(0), column_ids,
                        expr_types, values, index_searchable);
    GetPredicateColumns(schema, expression->GetModifiableChild(1), column_ids,
                        expr_types, values, index_searchable);
  }
}

static std::unique_ptr<const planner::ProjectInfo> CreateHackProjection();
static std::shared_ptr<const peloton::catalog::Schema> CreateHackJoinSchema();

std::unique_ptr<planner::AbstractPlan>
SimpleOptimizer::CreateHackingNestedLoopJoinPlan(
    const parser::SelectStatement* statement) {
  /*
  * "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT"
  + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " +
  TPCCConstants.TABLENAME_STOCK
        + " WHERE OL_W_ID = ?"
        + " AND OL_D_ID = ?"
        + " AND OL_O_ID < ?"
        + " AND OL_O_ID >= ?"
        + " AND S_W_ID = ?"
        + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?"
  */

  // Order_line is the outer table
  auto orderline_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "order_line");

  // Stock is the inner table
  auto stock_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "stock");

  // Manually constructing the predicate....
  expression::ParameterValueExpression* params[6];
  for (int i = 0; i < 6; ++i)
    params[i] = new expression::ParameterValueExpression(i);

  // Predicate for order_line table
  char ol_w_id_name[] = "ol_w_id";
  char ol_d_id_name[] = "ol_d_id";
  char ol_o_id_1_name[] = "ol_o_id";
  char ol_o_id_2_name[] = "ol_o_id";
  auto ol_w_id = new expression::TupleValueExpression(ol_w_id_name);
  auto ol_d_id = new expression::TupleValueExpression(ol_d_id_name);
  auto ol_o_id_1 = new expression::TupleValueExpression(ol_o_id_1_name);
  auto ol_o_id_2 = new expression::TupleValueExpression(ol_o_id_2_name);

  auto predicate1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, ol_w_id, params[0]);
  auto predicate2 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, ol_d_id, params[1]);
  auto predicate3 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_LESSTHAN, ol_o_id_1, params[2]);
  auto predicate4 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, ol_o_id_2, params[3]);

  auto predicate6 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, predicate1, predicate2);
  auto predicate7 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, predicate3, predicate4);
  auto predicate8 = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, predicate6, predicate7);

  // Get the index scan descriptor
  bool index_searchable;
  std::vector<oid_t> predicate_column_ids = {};
  std::vector<ExpressionType> predicate_expr_types;
  std::vector<type::Value> predicate_values;
  std::vector<oid_t> column_ids = {4};  // OL_I_ID
  std::vector<expression::AbstractExpression*> runtime_keys;

  // Get predicate ids, types, and values
  GetPredicateColumns(orderline_table->GetSchema(), predicate8,
                      predicate_column_ids, predicate_expr_types,
                      predicate_values, index_searchable);

  // Create index scan descpriptor
  auto index = orderline_table->GetIndex(0);  // Primary index
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, predicate_column_ids, predicate_expr_types, predicate_values,
      runtime_keys);

  // Remove redundant predicate that index can search
  auto tailored_predicate =
      expression::ExpressionUtil::RemoveTermsWithIndexedColumns(predicate8,
                                                                index);
  if (tailored_predicate != predicate8) delete predicate8;

  // Create the index scan plan for ORDER_LINE
  std::unique_ptr<planner::IndexScanPlan> orderline_scan_node(
      new planner::IndexScanPlan(orderline_table, tailored_predicate,
                                 column_ids, index_scan_desc, false));
  LOG_DEBUG("Index scan for order_line plan created");

  // predicate for scanning stock table
  char s_quantity_name[] = "s_quantity";

  auto s_quantity = new expression::TupleValueExpression(s_quantity_name);

  auto predicate12 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_LESSTHAN, s_quantity, params[5]);

  predicate_column_ids = {0, 1};  // S_W_ID, S_I_ID
  predicate_expr_types = {ExpressionType::COMPARE_EQUAL,
                          ExpressionType::COMPARE_EQUAL};

  // s_w_id the 5th parameter.
  // s_i_id is a fake value, which will be updated when invoke index_update
  predicate_values = {type::ValueFactory::GetParameterOffsetValue(4).Copy(),
                      type::ValueFactory::GetIntegerValue(0).Copy()};

  // output of stock
  column_ids = {1};  // S_I_ID

  auto index_stock = stock_table->GetIndex(0);  // primary_index
  planner::IndexScanPlan::IndexScanDesc index_scan_desc2(
      index_stock, predicate_column_ids, predicate_expr_types, predicate_values,
      runtime_keys);

  // Create the index scan plan for STOCK
  std::unique_ptr<planner::IndexScanPlan> stock_scan_node(
      new planner::IndexScanPlan(stock_table, predicate12, column_ids,
                                 index_scan_desc2, false));
  LOG_DEBUG("Index scan plan for STOCK created");

  auto projection = CreateHackProjection();
  auto schema = CreateHackJoinSchema();

  std::vector<oid_t> join_column_ids_left = {0};   // I_I_ID in the left result
  std::vector<oid_t> join_column_ids_right = {0};  // S_I_ID in the right result

  // Create hash join plan node.
  std::unique_ptr<planner::NestedLoopJoinPlan> nested_join_plan_node(
      new planner::NestedLoopJoinPlan(
          JoinType::INNER, nullptr, std::move(projection), schema,
          join_column_ids_left, join_column_ids_right));

  // Add left and right
  nested_join_plan_node->AddChild(std::move(orderline_scan_node));
  nested_join_plan_node->AddChild(std::move(stock_scan_node));

  expression::TupleValueExpression* left_table_attr_1 =
      new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);

  planner::AggregatePlan::AggTerm agg_term(
      ParserExpressionNameToExpressionType("count"), left_table_attr_1, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  agg_terms.push_back(agg_term);

  std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, 0);
  std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
      std::make_pair(0, inner_pair);
  DirectMapList direct_map_list;
  direct_map_list.emplace_back(outer_pair);
  LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
            outer_pair.second.first, outer_pair.second.second);

  LOG_TRACE("Creating a ProjectInfo");
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  std::vector<oid_t> group_by_columns;
  expression::AbstractExpression* having = nullptr;
  std::unique_ptr<const expression::AbstractExpression> predicate(having);

  auto column = catalog::Column(type::Type::INTEGER,
                                type::Type::GetTypeSize(type::Type::INTEGER),
                                "COL_0",  // COL_A should be
                                          // used only when
                                          // there is no AS
                                true);
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema({column}));
  LOG_DEBUG("Output Schema Info: %s",
            output_table_schema.get()->GetInfo().c_str());
  std::unique_ptr<planner::AggregatePlan> agg_plan(new planner::AggregatePlan(
      std::move(proj_info), std::move(predicate), std::move(agg_terms),
      std::move(group_by_columns), output_table_schema, AggregateType::PLAIN));
  LOG_DEBUG("Aggregation plan constructed");

  agg_plan->AddChild(std::move(nested_join_plan_node));

  for (auto col : *statement->select_list) {
    expression::ExpressionUtil::TransformExpression(
        (catalog::Schema*)stock_table->GetSchema(), col);
  }

  return std::move(agg_plan);
}

std::unique_ptr<const planner::ProjectInfo> CreateHackProjection() {
  // Create the plan node
  TargetList target_list;
  DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  DirectMap direct_map1 = std::make_pair(0, std::make_pair(1, 0));
  direct_map_list.push_back(direct_map1);

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

std::shared_ptr<const peloton::catalog::Schema> CreateHackJoinSchema() {
  auto column = catalog::Column(type::Type::INTEGER,
                                type::Type::GetTypeSize(type::Type::INTEGER),
                                "S_I_ID", 1);

  column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, "not_null"));
  return std::shared_ptr<const peloton::catalog::Schema>(
      new catalog::Schema({column}));
}

std::unique_ptr<planner::AbstractPlan> SimpleOptimizer::CreateJoinPlan(
    parser::SelectStatement* select_stmt) {
  // assuming no aggregation

  LOG_DEBUG("Create Join Plan");
  auto left_table = catalog::Catalog::GetInstance()->GetTableWithName(
      select_stmt->from_table->join->left->GetDatabaseName(),
      select_stmt->from_table->join->left->GetTableName());
  auto right_table = catalog::Catalog::GetInstance()->GetTableWithName(
      select_stmt->from_table->join->right->GetDatabaseName(),
      select_stmt->from_table->join->right->GetTableName());

  JoinType join_type = select_stmt->from_table->join->type;
  std::unique_ptr<const peloton::expression::AbstractExpression> join_condition(
      select_stmt->from_table->join->condition->Copy());
  auto join_for_update = select_stmt->is_for_update;

  std::unique_ptr<planner::SeqScanPlan> left_SelectPlan(
      new planner::SeqScanPlan(left_table, nullptr, {}, join_for_update));
  std::unique_ptr<planner::SeqScanPlan> right_SelectPlan(
      new planner::SeqScanPlan(right_table, nullptr, {}, join_for_update));

  auto left_schema = left_table->GetSchema();
  auto right_schema = right_table->GetSchema();

  // Get the key column name based on the join condition
  auto left_key_col_name =
      static_cast<expression::TupleValueExpression*>(
          join_condition->GetModifiableChild(0))->GetColumnName();
  auto right_key_col_name =
      static_cast<expression::TupleValueExpression*>(
          join_condition->GetModifiableChild(1))->GetColumnName();
  // Generate hash for right table
  auto right_key = expression::ExpressionUtil::ConvertToTupleValueExpression(
      right_schema, right_key_col_name);
  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(right_key);

  // Create hash plan node
  std::unique_ptr<planner::HashPlan> hash_plan_node(
      new planner::HashPlan(hash_keys));
  hash_plan_node->AddChild(std::move(right_SelectPlan));

  std::vector<catalog::Column> output_table_columns = {};
  // expressions to evaluate
  TargetList tl = TargetList();
  // columns which can be returned directly
  DirectMapList dml = DirectMapList();
  auto& select_list = *select_stmt->getSelectList();
  int i = 0;
  std::vector<const catalog::Schema*> schemas = {left_schema, right_schema};
  // This is introduced to check the name of the table while building dml
  std::vector<std::pair<const std::string, const catalog::Schema*>> schemas_ps =
      {{left_table->GetName(), left_schema},
       {right_table->GetName(), right_schema}};

  // SELECT * FROM A JOIN B
  if (select_list[0]->GetExpressionType() == ExpressionType::STAR) {
    auto& left_cols = left_schema->GetColumns();
    auto& right_cols = right_schema->GetColumns();
    output_table_columns = left_cols;
    output_table_columns.insert(output_table_columns.end(), right_cols.begin(),
                                right_cols.end());
    int left_col_cnt = left_cols.size();
    int right_col_cnt = right_cols.size();
    for (int j = 0; j < left_col_cnt; j++) {
      dml.push_back(DirectMap(i++, std::make_pair(0, j)));
    }
    for (int j = 0; j < right_col_cnt; j++) {
      dml.push_back(DirectMap(i++, std::make_pair(1, j)));
    }
  }

  // SELECT col1, col2, fun1, ... FROM A JOIN B
  else {
    for (auto expr : select_list) {
      expression::ExpressionUtil::TransformExpression(schemas, expr);

      auto expr_type = expr->GetExpressionType();
      // Column experssion
      if (expr_type == ExpressionType::VALUE_TUPLE) {
        auto tup_expr = (expression::TupleValueExpression*)expr;
        oid_t old_col_id = -1;
        catalog::Column column;

        for (int schema_index = 0; schema_index < 2; schema_index++) {
          auto& schema = schemas_ps[schema_index];
          old_col_id = schema.second->GetColumnID(tup_expr->GetColumnName());
          if (old_col_id != (oid_t)-1 &&
              schema.first == tup_expr->GetTableName()) {
            column = schema.second->GetColumn(old_col_id);
            output_table_columns.push_back(column);
            dml.push_back(
                DirectMap(i, std::make_pair(schema_index, old_col_id)));
            break;
          }
        }
      } else if (expr_type == ExpressionType::STAR) {
        for (int schema_index = 0; schema_index < 2; schema_index++) {
          auto& schema = schemas[schema_index];
          auto& cols = schema->GetColumns();
          output_table_columns.insert(output_table_columns.end(), cols.begin(),
                                      cols.end());
          for (int j = 0; j < (int)cols.size(); j++) {
            dml.push_back(DirectMap(i++, std::make_pair(schema_index, j)));
          }
        }
      }
      // Function Ref
      else {
        planner::DerivedAttribute attribute;
        attribute.expr = expr->Copy();
        attribute.attribute_info.type = attribute.expr->GetValueType();
        tl.push_back(Target(i, attribute));
        output_table_columns.push_back(catalog::Column(
            expr->GetValueType(), type::Type::GetTypeSize(expr->GetValueType()),
            "expr" + std::to_string(i)));
      }
      i++;
    }
  }

  std::shared_ptr<const peloton::catalog::Schema> schema(
      new catalog::Schema(output_table_columns));
  std::unique_ptr<planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(std::move(tl), std::move(dml)));
  LOG_DEBUG("Index scan column size: %ld\n", output_table_columns.size());
  LOG_DEBUG("Schema info: %s", schema->GetInfo().c_str());
  // // Create hash join plan node.
  std::unique_ptr<const peloton::expression::AbstractExpression> predicates =
      nullptr;
  if (select_stmt->where_clause != nullptr)
    predicates = std::unique_ptr<const peloton::expression::AbstractExpression>(
        select_stmt->where_clause->Copy());

  auto left_hash_key = peloton::expression::ExpressionUtil::
      ConvertToTupleValueExpression(left_schema, left_key_col_name);
  std::vector<std::unique_ptr<const expression::AbstractExpression>> lhash_keys;
  lhash_keys.emplace_back(left_hash_key);

  auto right_hash_key = peloton::expression::ExpressionUtil::
      ConvertToTupleValueExpression(right_schema, right_key_col_name);
  std::vector<std::unique_ptr<const expression::AbstractExpression>> rhash_keys;
  rhash_keys.emplace_back(right_hash_key);

  std::unique_ptr<planner::HashJoinPlan> hash_join_plan_node(
      new planner::HashJoinPlan(join_type, std::move(predicates),
                                std::move(proj_info), schema, lhash_keys,
                                rhash_keys));

  // index only works on comparison with a constant
  hash_join_plan_node->AddChild(std::move(left_SelectPlan));
  hash_join_plan_node->AddChild(std::move(hash_plan_node));
  return std::move(hash_join_plan_node);
}

void SimpleOptimizer::SetIndexScanFlag(planner::AbstractPlan* select_plan,
                                       uint64_t limit, uint64_t offset,
                                       bool descent) {
  LOG_TRACE("Setting index scan flag.");
  // Set the flag for the underlying index scan plan
  planner::IndexScanPlan* index_scan_plan = nullptr;

  // child_SelectPlan is projection plan or scan plan
  if (select_plan->GetPlanNodeType() == PlanNodeType::PROJECTION) {
    // it's child is index_scan or seq_scan. Only index_scan is set
    if (select_plan->GetChildren()[0]->GetPlanNodeType() ==
        PlanNodeType::INDEXSCAN) {
      index_scan_plan =
          (planner::IndexScanPlan*)select_plan->GetChildren()[0].get();
    }
  }
  // otherwise child_SelectPlan itself is scan plan
  else {
    // child_SelectPlan is index_scan or seq_scan
    if (select_plan->GetPlanNodeType() == PlanNodeType::INDEXSCAN) {
      index_scan_plan = (planner::IndexScanPlan*)select_plan;
    }
  }

  if (index_scan_plan != nullptr) {
    LOG_TRACE("Set index scan plan");
    index_scan_plan->SetLimit(true);
    index_scan_plan->SetLimitNumber(limit);
    index_scan_plan->SetLimitOffset(offset);
    index_scan_plan->SetDescend(descent);
  }

  LOG_TRACE("Set Index scan flag is done.");
}

bool SimpleOptimizer::UnderlyingSameOrder(planner::AbstractPlan* select_plan,
                                          oid_t orderby_column_id,
                                          bool order_by_descending) {
  planner::IndexScanPlan* index_scan_plan = nullptr;

  // Check whether underlying node is index scan
  // Child_SelectPlan is projection plan or scan plan
  if (select_plan->GetPlanNodeType() == PlanNodeType::PROJECTION) {
    // it's child is index_scan or seq_scan. Only index_scan is set
    if (select_plan->GetChildren()[0]->GetPlanNodeType() ==
        PlanNodeType::INDEXSCAN) {
      index_scan_plan =
          (planner::IndexScanPlan*)select_plan->GetChildren()[0].get();
    }
  }
  // otherwise child_SelectPlan itself is scan plan
  else {
    // child_SelectPlan is index_scan or seq_scan
    if (select_plan->GetPlanNodeType() == PlanNodeType::INDEXSCAN) {
      index_scan_plan = (planner::IndexScanPlan*)select_plan;
    }
  }

  // If the underling node is not index scan return false
  if (index_scan_plan == nullptr) {
    LOG_TRACE("underling node is not index scan");
    return false;
  }

  // Check whether index scan output has the same ordering with order_by
  if (index_scan_plan->GetDescend() != order_by_descending) {
    LOG_TRACE("index scan output does not have the same ordering");
    return false;
  }

  // Check whether all predicates types of index scan are equal
  for (auto type : index_scan_plan->GetExprTypes()) {
    if (type != ExpressionType::COMPARE_EQUAL) {
      LOG_TRACE("predicates types of index scan are not equal");
      return false;
    }
  }

  // Check whether order_by column_id is inside the index ids. If yes, we
  // directly return true
  for (auto index_id : index_scan_plan->GetKeyColumnIds()) {
    if (index_id == orderby_column_id) {
      LOG_TRACE("order_by column_id is inside the index ids");
      return true;
    }
  }

  // Check whether order_by column_id follows the index ids
  uint64_t size = index_scan_plan->GetKeyColumnIds().size();

  // Get the index_id following key column ids
  if (size >=
      index_scan_plan->GetIndex()->GetMetadata()->GetKeyAttrs().size()) {
    LOG_TRACE("size of index scan key ids is larger or eqaul than index ids");
    return false;
  }

  oid_t physical_column_id =
      index_scan_plan->GetIndex()->GetMetadata()->GetKeyAttrs()[size];

  // Whether the order by id is the same with the following index id
  if (physical_column_id != orderby_column_id) {
    LOG_TRACE("order by id (%u) is not equal to physical_column_id (%u)",
              orderby_column_id, physical_column_id);
    return false;
  }

  // All the checking is done, return true
  LOG_TRACE("All checking is done, so ordering is the same");
  return true;
}

std::unique_ptr<planner::AbstractPlan> SimpleOptimizer::CreateOrderByPlan(
    parser::SelectStatement* select_stmt, planner::AbstractPlan* child_plan,
    catalog::Schema* schema, std::vector<oid_t> column_ids, bool is_star) {
  auto schema_columns = schema->GetColumns();
  std::vector<oid_t> keys;
  // The order_by plan is oblivious to the type of query, so,
  // the columns must be manually added for it.
  if (is_star) {
    for (size_t column_ctr = 0; column_ctr < schema_columns.size();
         column_ctr++) {
      keys.push_back(column_ctr);
    }
  } else {
    for (size_t column_ctr = 0; column_ctr < select_stmt->select_list->size();
         column_ctr++) {
      keys.push_back(column_ctr);
    }
  }
  std::vector<bool> flags;
  if (select_stmt->order->types->at(0) == 0) {
    flags.push_back(false);
  } else {
    flags.push_back(true);
  }
  std::vector<oid_t> key;
  std::string sort_col_name(
      ((expression::TupleValueExpression*)select_stmt->order->exprs->at(0))
          ->GetColumnName());
  for (size_t column_ctr = 0; column_ctr < schema_columns.size();
       column_ctr++) {
    std::string col_name(schema_columns.at(column_ctr).GetName());
    if (col_name == sort_col_name) {
      if (is_star) {
        // The whole schema is already added, and column_ids
        // doesn't represent faithfully the columns.
        key.push_back(column_ctr);
      } else {
        // The column_ctr is not reliable anymore given that we are
        // looking to the whole schema.
        // Since the columns were added in the column_ids, it is safe
        // to retrieve only those indexes.
        auto iter = find(column_ids.begin(), column_ids.end(), column_ctr);
        auto column_offset = std::distance(column_ids.begin(), iter);
        key.push_back(column_offset);
      }
    }
  }

  std::unique_ptr<planner::OrderByPlan> order_by_plan(
      new planner::OrderByPlan(key, flags, keys));

  // Whether underlying child's output has the same order with the
  // order_by clause.
  LOG_TRACE("order by column id is %d", schema->GetColumnID(sort_col_name));
  if (UnderlyingSameOrder(child_plan, schema->GetColumnID(sort_col_name),
                          flags.front()) == true) {
    LOG_TRACE(
        "Underlying plan has the same ordering output with"
        "order_by plan without limit");
    order_by_plan->SetUnderlyingOrder(true);
  }
  return std::move(order_by_plan);
}

std::unique_ptr<planner::AbstractPlan> SimpleOptimizer::CreateOrderByLimitPlan(
    parser::SelectStatement* select_stmt, planner::AbstractPlan* child_plan,
    catalog::Schema* schema, std::vector<oid_t> column_ids, bool is_star) {
  LOG_TRACE("OrderBy + Limit query");
  auto abstract_plan =
      CreateOrderByPlan(select_stmt, child_plan, schema, column_ids, is_star);
  // Cast needed for desired fields.
  auto order_by_plan = static_cast<planner::OrderByPlan*>(abstract_plan.get());
  LOG_TRACE("Set order by offset");
  // Get offset
  int offset = select_stmt->limit->offset;
  if (offset < 0) {
    offset = 0;
  }

  // Set the flag for the underlying index scan plan to accelerate the
  // limit operation speed. That is to say the limit flags are passed
  // to index, then index returns the tuples matched with the limit
  SetIndexScanFlag(child_plan, select_stmt->limit->limit, offset,
                   order_by_plan->GetDescendFlags().front());

  // Whether underlying child's output has the same order
  // with the order_by clause
  LOG_TRACE("order by column id is %d", order_by_plan->GetSortKeys().front());
  // This step has a little redundance with the order_by plan
  // but changes more stuff inside.
  if (UnderlyingSameOrder(child_plan, order_by_plan->GetSortKeys().front(),
                          order_by_plan->GetDescendFlags().front()) == true) {
    LOG_TRACE(
        "Underlying plan has the same ordering output with"
        "order_by plan with limit");
    order_by_plan->SetUnderlyingOrder(true);
    order_by_plan->SetLimit(true);
    order_by_plan->SetLimitNumber(select_stmt->limit->limit);
    order_by_plan->SetLimitOffset(offset);
  }

  // Create limit_plan
  std::unique_ptr<planner::LimitPlan> limit_plan(
      new planner::LimitPlan(select_stmt->limit->limit, offset));
  // Adding the unique_ptr
  limit_plan->AddChild(std::move(abstract_plan));
  return std::move(limit_plan);
}
}  // namespace optimizer
}  // namespace peloton
