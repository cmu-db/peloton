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

#include "catalog/schema.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/parser_expression.h"
#include "expression/string_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/tuple_value_expression.h"

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

    if (expression->GetLeft()) {
      LOG_TRACE("expression->left: %s",
                expression->GetLeft()->GetInfo().c_str());
      if (expression->GetLeft()->GetExpressionType() ==
          EXPRESSION_TYPE_VALUE_PARAMETER) {
        // left expression is parameter
        auto left = (ParameterValueExpression *)expression->GetLeft();
        auto value =
            new ConstantValueExpression(values->at(left->GetValueIdx()));
        LOG_TRACE("left in vector type: %s",
                  values->at(left->GetValueIdx()).GetInfo().c_str());
        LOG_TRACE("Setting parameter %u to value %s", left->GetValueIdx(),
                  value->GetValue().GetInfo().c_str());
        delete left;
        expression->setLeftExpression(value);
      } else {
        ConvertParameterExpressions(expression->GetModifiableLeft(), values,
                                    schema);
      }
    }

    if (expression->GetRight()) {
      LOG_TRACE("expression->right: %s",
                expression->GetRight()->GetInfo().c_str());
      if (expression->GetRight()->GetExpressionType() ==
          EXPRESSION_TYPE_VALUE_PARAMETER) {
        // right expression is parameter
        auto right = (ParameterValueExpression *)expression->GetRight();
        LOG_TRACE("right in vector type: %s",
                  values->at(right->GetValueIdx()).GetInfo().c_str());
        auto value =
            new ConstantValueExpression(values->at(right->GetValueIdx()));
        LOG_TRACE("Setting parameter %u to value %s", right->GetValueIdx(),
                  value->GetValue()
                      .GetInfo()
                      .c_str());
        delete right;
        expression->setRightExpression(value);
      } else {
        ConvertParameterExpressions(expression->GetModifiableRight(), values,
                                    schema);
      }
    }
  }

  /**
   * This function replaces all COLUMN_REF expressions with TupleValue
   * expressions
   */
  static void ReplaceColumnExpressions(
      catalog::Schema *schema, expression::AbstractExpression *expression) {
    LOG_TRACE("Expression Type --> %s",
              ExpressionTypeToString(expression->GetExpressionType()).c_str());
    if (expression->GetLeft() != nullptr) {
      LOG_TRACE(
          "Left Type --> %s",
          ExpressionTypeToString(expression->GetLeft()->GetExpressionType())
              .c_str());
      if (expression->GetLeft()->GetExpressionType() ==
          EXPRESSION_TYPE_COLUMN_REF) {
        auto expr = expression->GetModifiableLeft();
        std::string col_name(expr->GetName());
        LOG_TRACE("Column name: %s", col_name.c_str());
        delete expr;
        expression->setLeftExpression(
            expression::ExpressionUtil::ConvertToTupleValueExpression(
                schema, col_name));

      } else {
        ReplaceColumnExpressions(schema, expression->GetModifiableLeft());
      }
    }

    if (expression->GetRight() != nullptr) {
      LOG_TRACE(
          "Right Type --> %s",
          ExpressionTypeToString(expression->GetRight()->GetExpressionType())
              .c_str());
      if (expression->GetRight()->GetExpressionType() ==
          EXPRESSION_TYPE_COLUMN_REF) {
        auto expr = expression->GetModifiableRight();
        std::string col_name(expr->GetName());
        LOG_TRACE("Column name: %s", col_name.c_str());
        delete expr;
        expression->setRightExpression(
            expression::ExpressionUtil::ConvertToTupleValueExpression(
                schema, col_name));
      } else {
        ReplaceColumnExpressions(schema, expression->GetModifiableRight());
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
      AbstractExpression *expr3, UNUSED_ATTRIBUTE AbstractExpression *expr4) {
    switch (type) {
      case (EXPRESSION_TYPE_ASCII):
        return new AsciiExpression(expr1);
      case (EXPRESSION_TYPE_CHAR):
        return new ChrExpression(expr1);
      case (EXPRESSION_TYPE_SUBSTR):
        return new SubstrExpression(expr1, expr2, expr3);
      case (EXPRESSION_TYPE_CHAR_LEN):
        return new CharLengthExpression(expr1);
      case (EXPRESSION_TYPE_CONCAT):
        return new ConcatExpression(expr1, expr2);
      case (EXPRESSION_TYPE_OCTET_LEN):
        return new OctetLengthExpression(expr1);
      case (EXPRESSION_TYPE_REPEAT):
        return new RepeatExpression(expr1, expr2);
      case (EXPRESSION_TYPE_REPLACE):
        return new ReplaceExpression(expr1, expr2, expr3);
      case (EXPRESSION_TYPE_LTRIM):
        return new LTrimExpression(expr1, expr2);
      case (EXPRESSION_TYPE_RTRIM):
        return new RTrimExpression(expr1, expr2);
      case (EXPRESSION_TYPE_BTRIM):
        return new BTrimExpression(expr1, expr2);
      default:
        return new OperatorExpression(type, value_type, expr1, expr2);
    }
  }

  static AbstractExpression *ConjunctionFactory(
      ExpressionType type,
      const std::list<expression::AbstractExpression *> &child_exprs) {
    if (child_exprs.empty())
      return new ConstantValueExpression(common::ValueFactory::GetBooleanValue(1));
    AbstractExpression *left = nullptr;
    AbstractExpression *right = nullptr;
    for (auto expr : child_exprs) {
      left = expr;
      right = new ConjunctionExpression(type, left, right);
    }
    return left;
  }
};
}
}
