//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_util.h
//
// Identification: src/include/expression/expression_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <sstream>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "expression/aggregate_expression.h"
#include "expression/case_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "include/function/string_functions.h"
#include "expression/tuple_value_expression.h"
#include "index/index.h"

namespace peloton {
namespace expression {

class ExpressionUtil {
 public:
  /**
   * This function generates a TupleValue expression from the column name
   */
  static AbstractExpression *ConvertToTupleValueExpression(
      catalog::Schema *schema, std::string column_name) {
    auto column_id = schema->GetColumnID(column_name);
    // If there is no column with this name
    if (column_id > schema->GetColumnCount()) {
      LOG_TRACE("Could not find column name %s in schema: %s",
                column_name.c_str(), schema->GetInfo().c_str());
      return nullptr;
    }
    LOG_TRACE("Column id in table: %u", column_id);
    expression::TupleValueExpression *expr =
        new expression::TupleValueExpression(schema->GetType(column_id), 0,
                                             column_id);
    return expr;
  }

  /**
   * This function converts each ParameterValueExpression in an expression tree
   * to
   * a value from the value vector
   */
  static void ConvertParameterExpressions(
      expression::AbstractExpression *expression,
      std::vector<type::Value> *values, catalog::Schema *schema) {
    LOG_TRACE("expression: %s", expression->GetInfo().c_str());

    if (expression->GetChild(0)) {
      LOG_TRACE("expression->left: %s",
                expression->GetChild(0)->GetInfo().c_str());
      if (expression->GetChild(0)->GetExpressionType() ==
          ExpressionType::VALUE_PARAMETER) {
        // left expression is parameter
        auto left = (ParameterValueExpression *)expression->GetChild(0);
        auto value =
            new ConstantValueExpression(values->at(left->GetValueIdx()));
        LOG_TRACE("left in vector type: %s",
                  values->at(left->GetValueIdx()).GetInfo().c_str());
        LOG_TRACE("Setting parameter %u to value %s", left->GetValueIdx(),
                  value->GetValue().GetInfo().c_str());
        expression->SetChild(0, value);
      } else {
        ConvertParameterExpressions(expression->GetModifiableChild(0), values,
                                    schema);
      }
    }

    if (expression->GetChild(1)) {
      LOG_TRACE("expression->right: %s",
                expression->GetChild(1)->GetInfo().c_str());
      if (expression->GetChild(1)->GetExpressionType() ==
          ExpressionType::VALUE_PARAMETER) {
        // right expression is parameter
        auto right = (ParameterValueExpression *)expression->GetChild(1);
        LOG_TRACE("right in vector type: %s",
                  values->at(right->GetValueIdx()).GetInfo().c_str());
        auto value =
            new ConstantValueExpression(values->at(right->GetValueIdx()));
        LOG_TRACE("Setting parameter %u to value %s", right->GetValueIdx(),
                  value->GetValue().GetInfo().c_str());
        expression->SetChild(1, value);
      } else {
        ConvertParameterExpressions(expression->GetModifiableChild(1), values,
                                    schema);
      }
    }
  }

  /*
   * This function removes all the terms related to indexed columns within an
   * expression. The expression passed in may be modified.
   *
   * NOTE:
   * 1. This should only be called after we check that the predicate is index
   * searchable. That means there are only "and" conjunctions.
   * 2. This will remove constant expression as well (something like 2+3=7). But
   * that shouldn't be included in the scan predicate anyway. We should handle
   * that special case.
   */
  static AbstractExpression *RemoveTermsWithIndexedColumns(
      AbstractExpression *expression, std::shared_ptr<index::Index> index) {
    LOG_TRACE("Expression Type --> %s",
              ExpressionTypeToString(expression->GetExpressionType()).c_str());

    size_t children_size = expression->GetChildrenSize();

    // Return itself if the TupleValueExpression is not indexed.
    if (expression->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto tuple_expr = (expression::TupleValueExpression *)expression;
      std::string col_name(tuple_expr->GetColumnName());

      LOG_TRACE("Check for TupleValueExpression with column %s",
                col_name.c_str());

      bool indexed = false;
      for (auto &indexed_column : index->GetKeySchema()->GetColumns()) {
        if (indexed_column.GetName() == col_name) {
          LOG_TRACE("Found indexed column");
          indexed = true;
          break;
        }
      }

      if (!indexed) return expression;
    }

    // LM: If it's an indexed TupleValueExpression or other
    // ConstantValueExpression/ParameterValueExpression, then it's removable.
    // Right now I couldn't think of other cases, so otherwise it's not handled.
    if (children_size == 0) {
      PL_ASSERT(
          expression->GetExpressionType() == ExpressionType::VALUE_TUPLE ||
          expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT ||
          expression->GetExpressionType() == ExpressionType::VALUE_PARAMETER);
      return nullptr;
    }

    // Otherwise it's an operator expression. We have to check the children.
    bool fully_removable = true;
    bool partial_removable = false;

    std::vector<AbstractExpression *> new_children;
    for (size_t i = 0; i < children_size; ++i) {
      auto child_expr = expression->GetModifiableChild(i);
      auto new_child = RemoveTermsWithIndexedColumns(child_expr, index);
      new_children.push_back(new_child);

      if (new_child != nullptr)
        fully_removable = false;
      else
        partial_removable = true;
    }

    LOG_TRACE("fully_removable: %d", fully_removable);
    LOG_TRACE("partial_removable: %d", partial_removable);

    if (fully_removable) return nullptr;

    // Only in an 'and' expression, we may be able to remove one literal
    if (expression->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
      // If one child is removable, return the other child
      if (partial_removable) {
        for (auto child : new_children)
          if (child != nullptr) return child->Copy();
      } else {
        // Otherwise replace the child expressions with tailored ones
        for (size_t i = 0; i < children_size; ++i) {
          if (expression->GetModifiableChild(i) != new_children[i]) {
            LOG_TRACE("Setting new child at idx: %ld", i);
            expression->SetChild(i, new_children[i]);
          }
        }
      }
    }

    return expression;
  }

  static AbstractExpression *TupleValueFactory(type::TypeId value_type,
                                               const int tuple_idx,
                                               const int value_idx) {
    return new TupleValueExpression(value_type, tuple_idx, value_idx);
  }

  static AbstractExpression *ConstantValueFactory(const type::Value &value) {
    return new ConstantValueExpression(value);
  }

  static AbstractExpression *ComparisonFactory(ExpressionType type,
                                               AbstractExpression *left,
                                               AbstractExpression *right) {
    return new ComparisonExpression(type, left, right);
  }

  static AbstractExpression *ConjunctionFactory(ExpressionType type,
                                                AbstractExpression *left,
                                                AbstractExpression *right) {
    return new ConjunctionExpression(type, left, right);
  }

  static AbstractExpression *OperatorFactory(ExpressionType type,
                                             type::TypeId value_type,
                                             AbstractExpression *left,
                                             AbstractExpression *right) {
    return new OperatorExpression(type, value_type, left, right);
  }

  static AbstractExpression *OperatorFactory(
      ExpressionType type, type::TypeId value_type, AbstractExpression *expr1,
      AbstractExpression *expr2, UNUSED_ATTRIBUTE AbstractExpression *expr3,
      UNUSED_ATTRIBUTE AbstractExpression *expr4) {
    switch (type) {
      default:
        return new OperatorExpression(type, value_type, expr1, expr2);
    }
  }

  static AbstractExpression *ConjunctionFactory(
      ExpressionType type,
      const std::list<expression::AbstractExpression *> &child_exprs) {
    if (child_exprs.empty())
      return new ConstantValueExpression(
          type::ValueFactory::GetBooleanValue(true));
    AbstractExpression *left = nullptr;
    AbstractExpression *right = nullptr;
    for (auto expr : child_exprs) {
      left = expr;
      right = new ConjunctionExpression(type, left, right);
    }
    return left;
  }

  inline static bool IsAggregateExpression(AbstractExpression *expr) {
    return IsAggregateExpression(expr->GetExpressionType());
  }

  inline static bool IsAggregateExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR:
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_AVG:
        return true;
      default:
        return false;
    }
  }

  inline static bool IsOperatorExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::OPERATOR_PLUS:
      case ExpressionType::OPERATOR_MINUS:
      case ExpressionType::OPERATOR_MULTIPLY:
      case ExpressionType::OPERATOR_DIVIDE:
      case ExpressionType::OPERATOR_CONCAT:
      case ExpressionType::OPERATOR_MOD:
      case ExpressionType::OPERATOR_CAST:
      case ExpressionType::OPERATOR_NOT:
      case ExpressionType::OPERATOR_IS_NULL:
      case ExpressionType::OPERATOR_EXISTS:
      case ExpressionType::OPERATOR_UNARY_MINUS:
        return true;
      default:
        return false;
    }
  }

  /**
   * Generate a pretty-printed string representation of the entire
   * Expresssion tree for the given root node
   */
  static std::string GetInfo(AbstractExpression *expr) {
    std::ostringstream os;
    std::string spacer("");
    GetInfo(expr, os, spacer);
    return os.str();
  }

 private:
  /**
   * Internal method for recursively traversing the expression tree
   * and generating debug output
   */
  static void GetInfo(const AbstractExpression *expr, std::ostringstream &os,
                      std::string spacer) {
    ExpressionType etype = expr->GetExpressionType();
    type::TypeId vtype = expr->GetValueType();

    // Basic Information
    os << spacer << "+ " << ExpressionTypeToString(etype) << "::"
       << "ValueType[" << TypeIdToString(vtype) << "]";

    switch (etype) {
      case ExpressionType::VALUE_CONSTANT: {
        const ConstantValueExpression *c_expr =
            dynamic_cast<const ConstantValueExpression *>(expr);
        os << " -> Value=" << c_expr->GetValue().ToString();
        break;
      }
      default: { break; }
    }  // SWITCH

    os << std::endl;
    spacer += "   ";
    for (int i = 0, cnt = expr->GetChildrenSize(); i < cnt; i++) {
      GetInfo(expr->GetChild(i), os, spacer);
    }
    return;
  }

 public:
  // TODO: Andy is commenting this out for now so that he can come back
  // 	 	 and fix it once we sort out our catalog information.
  //
  //  /**
  //   * Return a list of all of the catalog::Column objects referenced
  //   * in the given expression tree
  //   */
  //  static std::vector<catalog::Column> GetReferencedColumns(
  //		  catalog::Catalog *catalog,
  //		  AbstractExpression *expr) {
  //	  PL_ASSERT(catalog != nullptr);
  //	  PL_ASSERT(expr != nullptr);
  //	  std::vector<catalog::Column> columns;
  //
  //	  GetReferencedColumns(catalog, expr, columns);
  //
  //	  return (columns);
  //  }
  //
  // private:
  //
  //  static void GetReferencedColumns(
  //		  catalog::Catalog *catalog,
  //		  const AbstractExpression *expr,
  //		  std::vector<catalog::Column> &columns) {
  //
  //	  ExpressionType etype = expr->GetExpressionType();
  //	  if (etype == ExpressionType::VALUE_TUPLE) {
  //		  // TODO: Get the table + column name and grab the
  //		  // the handle from schema object!
  //		  const TupleValueExpression t_expr = dynamic_cast<const
  // TupleValueExpression*>(expr);
  //		  // catalog->Get
  //
  //	  }
  //
  //    }

 public:
  /**
  * Generate a set of table alias included in an expression
  */
  static void GenerateTableAliasSet(
      const AbstractExpression *expr,
      std::unordered_set<std::string> &table_alias_set) {
    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      table_alias_set.insert(
          reinterpret_cast<const TupleValueExpression *>(expr)->GetTableName());
      return;
    }
    for (size_t i = 0; i < expr->GetChildrenSize(); i++)
      GenerateTableAliasSet(expr->GetChild(i), table_alias_set);
  }

  /**
   * Convert all aggregate expression in the child expression tree
   * to tuple value expression with corresponding column offset
   * of the input child tuple. This is used for handling projection
   * on aggregate function (e.g. SELECT sum(a)+max(b) FROM ... GROUP BY ...)
   */
  static void ConvertAggExprToTvExpr(AbstractExpression *expr,
                                     ExprMap &child_expr_map) {
    for (size_t i = 0; i < expr->GetChildrenSize(); i++) {
      auto child_expr = expr->GetModifiableChild(i);
      if (IsAggregateExpression(child_expr->GetExpressionType())) {
        EvaluateExpression({child_expr_map}, child_expr);
        std::shared_ptr<AbstractExpression> probe_expr(
            std::shared_ptr<AbstractExpression>{}, child_expr);
        expr->SetChild(i,
                       new TupleValueExpression(child_expr->GetValueType(), 0,
                                                child_expr_map[probe_expr]));
      } else
        ConvertAggExprToTvExpr(child_expr, child_expr_map);
    }
  }

  /**
   * Generate a vector to store expressions in output order
   */
  static std::vector<std::shared_ptr<AbstractExpression>>
  GenerateOrderedOutputExprs(ExprMap &expr_map) {
    std::vector<std::shared_ptr<AbstractExpression>> ordered_expr(
        expr_map.size());
    for (auto iter : expr_map) ordered_expr[iter.second] = iter.first;
    return ordered_expr;
  }

  /**
   * Walks an expression trees and find all AggregationExprs subtrees.
   */
  static void GetAggregateExprs(
      std::vector<std::shared_ptr<AggregateExpression>> &aggr_exprs,
      AbstractExpression *expr) {
    std::vector<std::shared_ptr<TupleValueExpression>> dummy_tv_exprs;
    GetAggregateExprs(aggr_exprs, dummy_tv_exprs, expr);
  }

  /**
   * Walks an expression trees and find all AggregationExprs and TupleValueExprs
   * subtrees.
   */
  static void GetAggregateExprs(
      std::vector<std::shared_ptr<AggregateExpression>> &aggr_exprs,
      std::vector<std::shared_ptr<TupleValueExpression>> &tv_exprs,
      AbstractExpression *expr) {
    size_t children_size = expr->GetChildrenSize();
    if (IsAggregateExpression(expr->GetExpressionType()))
      aggr_exprs.emplace_back((AggregateExpression *)expr->Copy());
    else if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE)
      tv_exprs.emplace_back((TupleValueExpression *)expr->Copy());
    else {
      for (size_t i = 0; i < children_size; i++)
        GetAggregateExprs(aggr_exprs, tv_exprs, expr->GetModifiableChild(i));
    }
  }

  /**
   * Walks an expression trees and find all TupleValueExprs in the tree, add
   * them to a map for order preserving.
   */
  static void GetTupleValueExprs(ExprMap &expr_map, AbstractExpression *expr) {
    size_t children_size = expr->GetChildrenSize();
    for (size_t i = 0; i < children_size; i++)
      GetTupleValueExprs(expr_map, expr->GetModifiableChild(i));

    // Here we need a deep copy to void double delete subtree
    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE)
      expr_map.emplace(std::shared_ptr<AbstractExpression>(expr->Copy()),
                       expr_map.size());
  }

  /**
   * Walks an expression trees and find all TupleValueExprs in the tree
   */
  static void GetTupleValueExprs(ExprSet &expr_set, AbstractExpression *expr) {
    if (expr == nullptr) return;
    size_t children_size = expr->GetChildrenSize();
    for (size_t i = 0; i < children_size; i++)
      GetTupleValueExprs(expr_set, expr->GetModifiableChild(i));

    // Here we need a deep copy to void double delete subtree
    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE)
      expr_set.emplace(expr->Copy());
  }

  /**
   * Walks an expression trees. Set the value_idx for the leaf tuple_value_expr
   * Deduce the return value type of expression. Set the function ptr for
   * function expression.
   *
   * Plz notice: this function should only be used in the optimizer.
   * The following version TransformExpression will eventually be depracated
   */
  static void EvaluateExpression(const std::vector<ExprMap> &expr_maps,
                                 AbstractExpression *expr) {
    // To evaluate the return type, we need a bottom up approach.
    if (expr == nullptr) return;
    size_t children_size = expr->GetChildrenSize();
    for (size_t i = 0; i < children_size; i++)
      EvaluateExpression(expr_maps, expr->GetModifiableChild(i));

    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      // Point to the correct column returned in the logical tuple underneath
      // HACK: Need to construct shared_ptr for probing but not want the
      // shared_ptr to delete the object. Use alias constructor
      auto tup_expr = (TupleValueExpression *)expr;
      std::shared_ptr<AbstractExpression> probe_expr(
          std::shared_ptr<AbstractExpression>{}, tup_expr);
      size_t tuple_idx = 0;
      for (auto &expr_map : expr_maps) {
        auto iter = expr_map.find(probe_expr);
        if (iter != expr_map.end()) {
          tup_expr->SetValueIdx(iter->second, tuple_idx);
          break;
        }
        ++tuple_idx;
      }
    } else if (IsAggregateExpression(expr->GetExpressionType())) {
      auto aggr_expr = (AggregateExpression *)expr;
      std::shared_ptr<AbstractExpression> probe_expr(
          std::shared_ptr<AbstractExpression>{}, aggr_expr);
      auto &expr_map = expr_maps[0];
      auto iter = expr_map.find(probe_expr);
      if (iter != expr_map.end()) aggr_expr->SetValueIdx(iter->second);
    } else if (expr->GetExpressionType() ==
               ExpressionType::OPERATOR_CASE_EXPR) {
      auto case_expr = reinterpret_cast<expression::CaseExpression *>(expr);
      for (size_t i = 0; i < case_expr->GetWhenClauseSize(); i++) {
        EvaluateExpression(expr_maps, case_expr->GetWhenClauseCond(i));
      }
    }
  }

  /**
   * Extract join columns id from expr
   * For example, expr = (expr_1) AND (expr_2) AND (expr_3)
   * we only extract expr_i that have the format (l_table.a = r_table.b)
   * i.e. expr that is equality check for tuple columns from both of
   * the underlying tables
   *
   * remove set to true if we want to return a newly created expr with
   * join columns removed from expr
   */

  static expression::AbstractExpression *ExtractJoinColumns(
      std::vector<std::unique_ptr<const expression::AbstractExpression>> &
          l_column_exprs,
      std::vector<std::unique_ptr<const expression::AbstractExpression>> &
          r_column_exprs,
      const expression::AbstractExpression *expr) {
    if (expr == nullptr) return nullptr;
    if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
      auto left_expr =
          ExtractJoinColumns(l_column_exprs, r_column_exprs, expr->GetChild(0));

      auto right_expr =
          ExtractJoinColumns(l_column_exprs, r_column_exprs, expr->GetChild(1));

      expression::AbstractExpression *root = nullptr;

      if (left_expr == nullptr || right_expr == nullptr) {
        // Remove the CONJUNCTION_AND if left child or right child (or both)
        // is removed
        if (left_expr != nullptr) root = left_expr;
        if (right_expr != nullptr) root = right_expr;
      } else
        root = ConjunctionFactory(ExpressionType::CONJUNCTION_AND, left_expr,
                                  right_expr);

      return root;
    } else if (expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
      // TODO support arbitary comarison when executor add support
      // If left tuple and right tuple are from different child
      // then add to join column
      auto l_expr = expr->GetChild(0);
      auto r_expr = expr->GetChild(1);
      // TODO support arbitary expression
      if (l_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
          r_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        auto l_tuple_idx =
            reinterpret_cast<const expression::TupleValueExpression *>(l_expr)
                ->GetTupleId();
        auto r_tuple_idx =
            reinterpret_cast<const expression::TupleValueExpression *>(r_expr)
                ->GetTupleId();

        // If it can be removed then return nullptr
        if (l_tuple_idx != r_tuple_idx) {
          auto l_value_idx =
              reinterpret_cast<const expression::TupleValueExpression *>(l_expr)
                  ->GetColumnId();
          auto r_value_idx =
              reinterpret_cast<const expression::TupleValueExpression *>(r_expr)
                  ->GetColumnId();
          if (l_tuple_idx == 0) {
            l_column_exprs.emplace_back(new expression::TupleValueExpression(
                l_expr->GetValueType(), 0, l_value_idx));
            r_column_exprs.emplace_back(new expression::TupleValueExpression(
                r_expr->GetValueType(), 0, r_value_idx));
          } else {
            l_column_exprs.emplace_back(new expression::TupleValueExpression(
                l_expr->GetValueType(), 0, r_value_idx));
            r_column_exprs.emplace_back(new expression::TupleValueExpression(
                r_expr->GetValueType(), 0, l_value_idx));
          }
          return nullptr;
        }
      }
    }

    return expr->Copy();
  }


  /*
  */
  static bool GetPredicateForZoneMap(std::vector<std::unique_ptr<const expression::AbstractExpression>> &predicate_restrictions,
    const expression::AbstractExpression *expr) {
    if (expr == nullptr) {
      return false;
    }
    auto expr_type = expr->GetExpressionType();
    // If its and, split children and parse again
    if (expr_type == ExpressionType::CONJUNCTION_AND) {
      
      bool left_expr = GetPredicateForZoneMap(predicate_restrictions, expr->GetChild(0));
      bool right_expr = GetPredicateForZoneMap(predicate_restrictions, expr->GetChild(1));
      
      if ((!left_expr) || (!right_expr)) {
        return false;
      }
      return true;

    } else if (expr_type == ExpressionType::COMPARE_EQUAL ||
      expr_type == ExpressionType::COMPARE_LESSTHAN ||
      expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
      expr_type == ExpressionType::COMPARE_GREATERTHAN ||
      expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {

      predicate_restrictions.emplace_back(new expression::ComparisonExpression(
        expr_type, expr->GetModifiableChild(0), expr->GetModifiableChild(1) ));
      return true;
    }
    
    return false;
  }

  /*
   * Check whether two vectors of expression equal to each other.
   * ordered flag indicate whether the comparison should consider the order.
   * */
  static bool EqualExpressions(
      const std::vector<std::shared_ptr<expression::AbstractExpression>> &l,
      const std::vector<std::shared_ptr<expression::AbstractExpression>> &r,
      bool ordered = false) {
    if (l.size() != r.size()) return false;
    // Consider expression order in the comparison
    if (ordered) {
      size_t num_exprs = l.size();
      for (size_t i = 0; i < num_exprs; i++)
        if (!l[i]->Equals(r[i].get())) return false;
      return true;
    } else {
      ExprSet l_set, r_set;
      for (auto expr : l) l_set.insert(expr);
      for (auto expr : r) r_set.insert(expr);
      return l_set == r_set;
    }
  }
};
}
}
