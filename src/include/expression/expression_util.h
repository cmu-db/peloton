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

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "string_functions.h"

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
      expression::AbstractExpression *expression, std::vector<Value> *values,
      catalog::Schema *schema) {
    LOG_TRACE("expression: %s", expression->GetInfo().c_str());

    if (expression->GetChild(0)) {
      LOG_TRACE("expression->left: %s",
                expression->GetChild(0)->GetInfo().c_str());
      if (expression->GetChild(0)->GetExpressionType() ==
          EXPRESSION_TYPE_VALUE_PARAMETER) {
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
          EXPRESSION_TYPE_VALUE_PARAMETER) {
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

  static AbstractExpression *TupleValueFactory(common::Type::TypeId value_type,
                                               const int tuple_idx,
                                               const int value_idx) {
    return new TupleValueExpression(value_type, tuple_idx, value_idx);
  }

  static AbstractExpression *ConstantValueFactory(const common::Value &value) {
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
                                             common::Type::TypeId value_type,
                                             AbstractExpression *left,
                                             AbstractExpression *right) {
    return new OperatorExpression(type, value_type, left, right);
  }

  static AbstractExpression *OperatorFactory(
      ExpressionType type, common::Type::TypeId value_type,
      AbstractExpression *expr1, AbstractExpression *expr2,
      UNUSED_ATTRIBUTE AbstractExpression *expr3,
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
          common::ValueFactory::GetBooleanValue(true));
    AbstractExpression *left = nullptr;
    AbstractExpression *right = nullptr;
    for (auto expr : child_exprs) {
      left = expr;
      right = new ConjunctionExpression(type, left, right);
    }
    return left;
  }

  inline static bool IsAggregateExpression(ExpressionType type) {
    switch (type) {
      case EXPRESSION_TYPE_AGGREGATE_COUNT:
      case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
      case EXPRESSION_TYPE_AGGREGATE_SUM:
      case EXPRESSION_TYPE_AGGREGATE_MIN:
      case EXPRESSION_TYPE_AGGREGATE_MAX:
      case EXPRESSION_TYPE_AGGREGATE_AVG:
        return true;
      default:
        return false;
    }
  }

  inline static bool IsOperatorExpression(ExpressionType type) {
    switch (type) {
      case EXPRESSION_TYPE_OPERATOR_PLUS:
      case EXPRESSION_TYPE_OPERATOR_MINUS:
      case EXPRESSION_TYPE_OPERATOR_MULTIPLY:
      case EXPRESSION_TYPE_OPERATOR_DIVIDE:
      case EXPRESSION_TYPE_OPERATOR_CONCAT:
      case EXPRESSION_TYPE_OPERATOR_MOD:
      case EXPRESSION_TYPE_OPERATOR_CAST:
      case EXPRESSION_TYPE_OPERATOR_NOT:
      case EXPRESSION_TYPE_OPERATOR_IS_NULL:
      case EXPRESSION_TYPE_OPERATOR_EXISTS:
      case EXPRESSION_TYPE_OPERATOR_UNARY_MINUS:
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
    Type::TypeId vtype = expr->GetValueType();

    // Basic Information
    os << spacer << "+ " << ExpressionTypeToString(etype) << "::"
       << "ValueType[" << TypeIdToString(vtype) << "]";

    switch (etype) {
      case EXPRESSION_TYPE_VALUE_CONSTANT: {
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
  //	  if (etype == EXPRESSION_TYPE_VALUE_TUPLE) {
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
   * Walks an expression tree and fills in information about
   * columns and functions in their respective obejects
   */
  static void TransformExpression(catalog::Schema *schema,
                                  AbstractExpression *expr) {
    bool dummy;
    TransformExpression(nullptr, nullptr, expr, schema, dummy, false);
  }

  /**
   * This function walks an expression tree and fills in information about
   * columns and functions. Also generates a list of column ids we need to fetch
   * from the base tile groups. Simultaniously generates a mapping of the
   * original column
   * id to the id in the logical tiles returned by the base tile groups
   *
   * This function is useful in determining information used by projection plans
   */
  static void TransformExpression(
      std::unordered_map<oid_t, oid_t> &column_mapping,
      std::vector<oid_t> &column_ids, AbstractExpression *expr,
      const catalog::Schema &schema, bool &needs_projection) {
    TransformExpression(&column_mapping, &column_ids, expr, &schema,
                        needs_projection, true);
  }

 private:
  /**
   * this is a private function for transforming expressions as described
   * above
   *
   * find columns determines if we are building a column_mapping and
   * column_ids
   * or we are just transforming
   * the expressions
   */
  static void TransformExpression(
      std::unordered_map<oid_t, oid_t> *column_mapping,
      std::vector<oid_t> *column_ids, AbstractExpression *expr,
      const catalog::Schema *schema, bool &needs_projection,
      bool find_columns) {
    if (expr == nullptr) {
      return;
    }
    size_t num_children = expr->GetChildrenSize();
    // do dfs to transform all chilren
    for (size_t child = 0; child < num_children; child++) {
      TransformExpression(column_mapping, column_ids,
                          expr->GetModifiableChild(child), schema,
                          needs_projection, find_columns);
    }
    // if this is a column, we need to find if it is exists in the scema
    if (expr->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE &&
        expr->GetValueType() == Type::INVALID) {
      auto val_expr = (expression::TupleValueExpression *)expr;
      auto col_id = schema->GetColumnID(val_expr->GetColumnName());
      // exception if we can't find the requested column by name
      if (col_id == (oid_t)-1) {
        throw Exception("Column " + val_expr->GetColumnName() + " not found");
      }
      auto column = schema->GetColumn(col_id);
      // make sure the column we need is returned from the scan
      // and we know where it is (for projection)
      size_t mapped_position;
      if (find_columns) {
        if (column_mapping->count(col_id) == 0) {
          mapped_position = column_ids->size();
          column_ids->push_back(col_id);
          (*column_mapping)[col_id] = mapped_position;
        } else {
          mapped_position = (*column_mapping)[col_id];
        }
      } else {
        mapped_position = col_id;
      }
      auto type = column.GetType();
      // set the expression name to the alias if we have one
      if (val_expr->alias.size() > 0) {
        val_expr->expr_name_ = val_expr->alias;
      } else {
        val_expr->expr_name_ = val_expr->GetColumnName();
      }
      // point to the correct column returned in the logical tuple underneath
      val_expr->SetTupleValueExpressionParams(type, mapped_position, 0);
    }
    // if we have any expression besides column expressiona and star, we
    // need to add a projection node
    else if (expr->GetExpressionType() != EXPRESSION_TYPE_STAR) {
      needs_projection = true;
    }
    // if the expressio is a fucntion, do a lookup and make sure it exists
    if (expr->GetExpressionType() == EXPRESSION_TYPE_FUNCTION) {
      auto func_expr = (expression::FunctionExpression *)expr;
      auto catalog = catalog::Catalog::GetInstance();
      catalog::FunctionData func_data =
          catalog->GetFunction(func_expr->func_name_);
      func_expr->SetFunctionExpressionParameters(func_data.func_ptr_,
                                                 func_data.return_type_,
                                                 func_data.num_arguments_);
    }
    // make sure the return types for expressions are set correctly
    // this is useful in operator expressions
    expr->DeduceExpressionType();
  }
};
}
}
