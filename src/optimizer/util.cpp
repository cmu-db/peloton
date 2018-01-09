//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// util.cpp
//
// Identification: src/optimizer/util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/util.h"

#include "concurrency/transaction_manager_factory.h"
#include "catalog/query_metrics_catalog.h"
#include "expression/expression_util.h"
#include "planner/copy_plan.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {
namespace util {

// TODO(boweic): This legacy function seems to be a little bit hacky and may not
// work, we may want to further investigate it
/**
 * @breif This function checks whether the current expression can enable index
 *  scan for the statement. If it is index searchable, returns true and
 *  set the corresponding data structures that will be used in creating
 *  index scan node. Otherwise, returns false.
 *
 * @return Return if the current table is index-searchable
 */
bool CheckIndexSearchable(storage::DataTable *target_table,
                          expression::AbstractExpression *expression,
                          std::vector<oid_t> &key_column_ids,
                          std::vector<ExpressionType> &expr_types,
                          std::vector<type::Value> &values, oid_t &index_id) {
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
      int max_columns = 0;
      int index_index = 0;
      for (auto &column_set : target_table->GetIndexColumns()) {
        int matched_columns = 0;
        for (auto column_id : predicate_column_ids)
          if (column_set.find(column_id) != column_set.end()) matched_columns++;
        if (matched_columns > max_columns) {
          index_searchable = true;
          index_id = index_index;
          max_columns = matched_columns;
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

/**
 * @breif This function replaces all COLUMN_REF expressions with TupleValue
 *  expressions
 */
void GetPredicateColumns(const catalog::Schema *schema,
                         expression::AbstractExpression *expression,
                         std::vector<oid_t> &column_ids,
                         std::vector<ExpressionType> &expr_types,
                         std::vector<type::Value> &values,
                         bool &index_searchable) {
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
      auto expr = (expression::TupleValueExpression *)expression->GetChild(0);
      std::string col_name(expr->GetColumnName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      auto column_id = schema->GetColumnID(col_name);
      column_ids.push_back(column_id);
      expr_types.push_back(expression->GetExpressionType());

      if (right_type == ExpressionType::VALUE_CONSTANT) {
        values.push_back(
            reinterpret_cast<expression::ConstantValueExpression *>(
                expression->GetModifiableChild(1))
                ->GetValue());
        LOG_TRACE("Value Type: %d",
                  reinterpret_cast<expression::ConstantValueExpression *>(
                      expression->GetModifiableChild(1))
                      ->GetValueType());
      } else
        values.push_back(
            type::ValueFactory::GetParameterOffsetValue(
                reinterpret_cast<expression::ParameterValueExpression *>(
                    expression->GetModifiableChild(1))
                    ->GetValueIdx())
                .Copy());
      LOG_TRACE("Parameter offset: %s", (*values.rbegin()).GetInfo().c_str());
    }
  } else if (expression->GetChild(1)->GetExpressionType() ==
             ExpressionType::VALUE_TUPLE) {
    auto left_type = expression->GetChild(0)->GetExpressionType();
    if (left_type == ExpressionType::VALUE_CONSTANT ||
        left_type == ExpressionType::VALUE_PARAMETER) {
      auto expr = (expression::TupleValueExpression *)expression->GetChild(1);
      std::string col_name(expr->GetColumnName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      auto column_id = schema->GetColumnID(col_name);
      LOG_TRACE("Column id: %d", column_id);
      column_ids.push_back(column_id);
      expr_types.push_back(expression->GetExpressionType());

      if (left_type == ExpressionType::VALUE_CONSTANT) {
        values.push_back(
            reinterpret_cast<expression::ConstantValueExpression *>(
                expression->GetModifiableChild(1))
                ->GetValue());
        LOG_TRACE("Value Type: %d",
                  reinterpret_cast<expression::ConstantValueExpression *>(
                      expression->GetModifiableChild(0))
                      ->GetValueType());
      } else
        values.push_back(
            type::ValueFactory::GetParameterOffsetValue(
                reinterpret_cast<expression::ParameterValueExpression *>(
                    expression->GetModifiableChild(0))
                    ->GetValueIdx())
                .Copy());
      LOG_TRACE("Parameter offset: %s", (*values.rbegin()).GetInfo().c_str());
    }
  } else {
    GetPredicateColumns(schema, expression->GetModifiableChild(0), column_ids,
                        expr_types, values, index_searchable);
    GetPredicateColumns(schema, expression->GetModifiableChild(1), column_ids,
                        expr_types, values, index_searchable);
  }
}

/**
 * @brief Extract single table precates and multi-table predicates from the expr
 *
 * @param expr The original predicate
 * @param annotated_predicates The extracted conjunction predicates
 */
void ExtractPredicates(expression::AbstractExpression *expr,
                       std::vector<AnnotatedExpression> &annotated_predicates) {
  // Split a complex predicate into a set of predicates connected by AND.
  std::vector<expression::AbstractExpression *> predicates;
  SplitPredicates(expr, predicates);

  for (auto predicate : predicates) {
    std::unordered_set<std::string> table_alias_set;
    expression::ExpressionUtil::GenerateTableAliasSet(predicate,
                                                      table_alias_set);
    // Deep copy expression to avoid memory leak
    annotated_predicates.emplace_back(AnnotatedExpression(
        std::shared_ptr<expression::AbstractExpression>(predicate->Copy()),
        table_alias_set));
  }
}

/**
 * @breif Construct a qualified join predicate (contains a subset of alias in
 *  the table_alias_set)
 *  and remove the multitable expressions in the original join_predicates
 *
 * @return The final join predicate
 */
expression::AbstractExpression *ConstructJoinPredicate(
    std::unordered_set<std::string> &table_alias_set,
    MultiTablePredicates &join_predicates) {
  std::vector<std::shared_ptr<expression::AbstractExpression>> qualified_exprs;
  MultiTablePredicates new_join_predicates;
  for (auto predicate : join_predicates) {
    if (IsSubset(table_alias_set, predicate.table_alias_set))
      qualified_exprs.emplace_back(predicate.expr);
    else
      new_join_predicates.emplace_back(predicate);
  }
  join_predicates = std::move(new_join_predicates);
  return CombinePredicates(qualified_exprs);
}

/**
 * @breif Split conjunction expression tree into a vector of expressions with
 * AND
 */
void SplitPredicates(
    expression::AbstractExpression *expr,
    std::vector<expression::AbstractExpression *> &predicates) {
  if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
    // Traverse down the expression tree along conjunction
    for (size_t i = 0; i < expr->GetChildrenSize(); i++) {
      SplitPredicates(expr->GetModifiableChild(i), predicates);
    }
  } else {
    // Find an expression that is the child of conjunction expression
    predicates.push_back(expr);
  }
}

/**
 * @breif Combine a vector of expressions with AND
 */
expression::AbstractExpression *CombinePredicates(
    std::vector<std::shared_ptr<expression::AbstractExpression>> predicates) {
  if (predicates.empty()) return nullptr;

  if (predicates.size() == 1) return predicates[0]->Copy();

  auto conjunction = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, predicates[0]->Copy(),
      predicates[1]->Copy());
  for (size_t i = 2; i < predicates.size(); i++) {
    conjunction = new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, conjunction, predicates[i]->Copy());
  }
  return conjunction;
}

/**
 * @breif Combine a vector of expressions with AND
 */
expression::AbstractExpression *CombinePredicates(
    std::vector<AnnotatedExpression> predicates) {
  if (predicates.empty()) return nullptr;

  if (predicates.size() == 1) return predicates[0].expr->Copy();

  auto conjunction = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, predicates[0].expr->Copy(),
      predicates[1].expr->Copy());
  for (size_t i = 2; i < predicates.size(); i++) {
    conjunction = new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, conjunction,
        predicates[i].expr->Copy());
  }
  return conjunction;
}

/**
 * @breif Check if there are any join columns in the join expression
 *  For example, expr = (expr_1) AND (expr_2) AND (expr_3)
 *  we only extract expr_i that have the format (l_table.a = r_table.b)
 *  i.e. expr that is equality check for tuple columns from both of
 *  the underlying tables
 */

bool ContainsJoinColumns(const std::unordered_set<std::string> &l_group_alias,
                         const std::unordered_set<std::string> &r_group_alias,
                         const expression::AbstractExpression *expr) {
  if (expr == nullptr) return false;
  if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
    if (ContainsJoinColumns(l_group_alias, r_group_alias, expr->GetChild(0)) ||
        ContainsJoinColumns(l_group_alias, r_group_alias, expr->GetChild(1)))
      return true;
    else
      return false;
  } else if (expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
    // TODO support arbitary comarison when executor add support
    // If left tuple and right tuple are from different child
    // then add to join column
    auto l_expr = expr->GetChild(0);
    auto r_expr = expr->GetChild(1);
    // TODO support arbitary expression
    if (l_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
        r_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto l_table_alias =
          reinterpret_cast<const expression::TupleValueExpression *>(l_expr)
              ->GetTableName();
      auto r_table_alias =
          reinterpret_cast<const expression::TupleValueExpression *>(r_expr)
              ->GetTableName();

      // If they're from different child, then join column != empty
      if ((l_group_alias.count(l_table_alias) &&
           r_group_alias.count(r_table_alias)) ||
          (l_group_alias.count(r_table_alias) &&
           r_group_alias.count(l_table_alias)))
        return true;
    }
  }

  return false;
}

std::unique_ptr<planner::AbstractPlan> CreateCopyPlan(
    parser::CopyStatement *copy_stmt) {
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

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      copy_stmt->cpy_table->GetDatabaseName(),
      copy_stmt->cpy_table->GetTableName(), txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<planner::SeqScanPlan> select_plan(
      new planner::SeqScanPlan(target_table, nullptr, {}, false));

  LOG_DEBUG("Sequential scan plan for copy created");

  // Attach it to the copy plan
  copy_plan->AddChild(std::move(select_plan));
  return copy_plan;
}

/**
 * @brief Construct the map from subquery column name to the actual expression
 *  at the subquery level, for example SELECT a FROM (SELECT a + b as a FROM
 *  test), we'll build the map {"a" -> a + b}
 *
 * @param select_list The select list of a subquery
 *
 * @return The mapping mentioned above
 */
std::unordered_map<std::string, std::shared_ptr<expression::AbstractExpression>>
ConstructSelectElementMap(
    std::vector<std::unique_ptr<expression::AbstractExpression>> &select_list) {
  std::unordered_map<std::string,
                     std::shared_ptr<expression::AbstractExpression>> res;
  for (auto &expr : select_list) {
    std::string alias;
    if (!expr->alias.empty()) {
      alias = expr->alias;
    } else if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto tv_expr =
          reinterpret_cast<expression::TupleValueExpression *>(expr.get());
      alias = tv_expr->GetColumnName();
    } else
      continue;
    std::transform(alias.begin(), alias.end(), alias.begin(), ::tolower);
    res[alias] = std::shared_ptr<expression::AbstractExpression>(expr->Copy());
  }
  return res;
};

/**
 * @brief Walk a predicate, transform the tuple value expression into the actual
 *  expression in the sub-query
 *
 * @param alias_to_expr_map The column name to expression map
 * @param expr the predicate
 *
 * @return the transformed predicate
 */
expression::AbstractExpression *TransformQueryDerivedTablePredicates(
    const std::unordered_map<std::string,
                             std::shared_ptr<expression::AbstractExpression>>
        &alias_to_expr_map,
    expression::AbstractExpression *expr) {
  if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
    auto new_expr =
        alias_to_expr_map
            .find(reinterpret_cast<expression::TupleValueExpression *>(expr)
                      ->GetColumnName())
            ->second;
    return new_expr->Copy();
  }
  auto child_size = expr->GetChildrenSize();
  for (size_t i = 0; i < child_size; i++) {
    expr->SetChild(i, TransformQueryDerivedTablePredicates(
                          alias_to_expr_map, expr->GetModifiableChild(i)));
  }
  return expr;
}

/**
 * @brief Walk through a set of join predicates, generate join keys base on the
 *  left/right table aliases set
 */
void ExtractEquiJoinKeys(
    const std::vector<AnnotatedExpression> join_predicates,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &left_keys,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &right_keys,
    const std::unordered_set<std::string> &left_alias,
    const std::unordered_set<std::string> &right_alias) {
  for (auto &expr_unit : join_predicates) {
    if (expr_unit.expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
      auto l_expr = expr_unit.expr->GetChild(0);
      auto r_expr = expr_unit.expr->GetChild(1);
      if (l_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
          r_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        auto l_tv_expr =
            reinterpret_cast<const expression::TupleValueExpression *>(l_expr);
        auto r_tv_expr =
            reinterpret_cast<const expression::TupleValueExpression *>(r_expr);
        if (left_alias.find(l_tv_expr->GetTableName()) != left_alias.end() &&
            right_alias.find(r_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys.emplace_back(l_tv_expr->Copy());
          right_keys.emplace_back(r_tv_expr->Copy());
        } else if (left_alias.find(r_tv_expr->GetTableName()) !=
                       left_alias.end() &&
                   right_alias.find(l_tv_expr->GetTableName()) !=
                       right_alias.end()) {
          left_keys.emplace_back(r_tv_expr->Copy());
          right_keys.emplace_back(l_tv_expr->Copy());
        }
      }
    }
  }
}

}  // namespace util
}  // namespace optimizer
}  // namespace peloton
