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

std::vector<AnnotatedExpression> ExtractPredicates(
    expression::AbstractExpression *expr,
    std::vector<AnnotatedExpression> annotated_predicates) {
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
  return annotated_predicates;
}

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
