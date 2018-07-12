//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// util.cpp
//
// Identification: src/optimizer/util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/selectivity.h"
#include "optimizer/stats/value_condition.h"
#include "optimizer/util.h"

#include "catalog/query_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/expression_util.h"

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

// Calculate the selectivity given the predicate and the stats of columns in the
// predicate
double CalculateSelectivityForPredicate(
    const std::shared_ptr<TableStats> predicate_table_stats,
    const expression::AbstractExpression *expr) {
  double selectivity = 1.f;
  if (predicate_table_stats->GetColumnCount() == 0 ||
      predicate_table_stats->GetColumnStats(0)->num_rows == 0) {
    return selectivity;
  }
  // Base case : Column Op Val
  if ((expr->GetChild(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
       (expr->GetChild(1)->GetExpressionType() ==
            ExpressionType::VALUE_CONSTANT ||
        expr->GetChild(1)->GetExpressionType() ==
            ExpressionType::VALUE_PARAMETER)) ||
      (expr->GetChild(1)->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
       (expr->GetChild(0)->GetExpressionType() ==
            ExpressionType::VALUE_CONSTANT ||
        expr->GetChild(0)->GetExpressionType() ==
            ExpressionType::VALUE_PARAMETER))) {
    int right_index =
        expr->GetChild(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE
            ? 1
            : 0;

    auto left_expr = expr->GetChild(1 - right_index);
    auto col_name =
        reinterpret_cast<const expression::TupleValueExpression *>(left_expr)
            ->GetColFullName();

    auto expr_type = expr->GetExpressionType();
    if (right_index == 0) {
      switch (expr_type) {
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          expr_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          break;
        case ExpressionType::COMPARE_LESSTHAN:
          expr_type = ExpressionType::COMPARE_GREATERTHAN;
          break;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          expr_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
          break;
        case ExpressionType::COMPARE_GREATERTHAN:
          expr_type = ExpressionType::COMPARE_LESSTHAN;
          break;
        default:
          break;
      }
    }

    type::Value value;
    if (expr->GetChild(right_index)->GetExpressionType() ==
        ExpressionType::VALUE_CONSTANT) {
      value = reinterpret_cast<expression::ConstantValueExpression *>(
                  expr->GetModifiableChild(right_index))
                  ->GetValue();
    } else {
      value = type::ValueFactory::GetParameterOffsetValue(
                  reinterpret_cast<expression::ParameterValueExpression *>(
                      expr->GetModifiableChild(right_index))
                      ->GetValueIdx())
                  .Copy();
    }
    ValueCondition condition(col_name, expr_type, value);
    selectivity =
        Selectivity::ComputeSelectivity(predicate_table_stats, condition);
  } else if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
             expr->GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
    double left_selectivity = CalculateSelectivityForPredicate(
        predicate_table_stats, expr->GetChild(0));
    double right_selectivity = CalculateSelectivityForPredicate(
        predicate_table_stats, expr->GetChild(1));
    if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
      selectivity = left_selectivity * right_selectivity;
    } else {
      selectivity = left_selectivity + right_selectivity -
                    left_selectivity * right_selectivity;
    }
  }
  return selectivity;
}

}  // namespace util
}  // namespace optimizer
}  // namespace peloton
