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
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/string_functions.h"
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
   * expression.
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
          if (child != nullptr) return child;
      } else {
        // Otherwise replace the child expressions with tailored ones
        for (size_t i = 0; i < children_size; ++i) {
          if (expression->GetModifiableChild(i) != new_children[i]) {
            LOG_TRACE("Setting new child at idx: %ld", i);
            expression->SetChild(i, new_children[i]->Copy());
          }
        }
      }
    }

    return expression;
  }

  static AbstractExpression *TupleValueFactory(type::Type::TypeId value_type,
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
                                             type::Type::TypeId value_type,
                                             AbstractExpression *left,
                                             AbstractExpression *right) {
    return new OperatorExpression(type, value_type, left, right);
  }

  static AbstractExpression *OperatorFactory(
      ExpressionType type, type::Type::TypeId value_type,
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
          type::ValueFactory::GetBooleanValue(true));
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
    type::Type::TypeId vtype = expr->GetValueType();

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
   * Walks an expression tree and fills in information about
   * columns and functions in their respective objects given a
   * set of schemas.
   */
  static void TransformExpression(std::vector<const catalog::Schema *> &schemas,
                                  AbstractExpression *expr) {
    bool dummy;
    TransformExpression(nullptr, nullptr, expr, schemas, dummy, false);
  }

  /**
   * Walks an expression tree and fills in information about
   * columns and functions in their respective objects given a
   * schema
   */
  static void TransformExpression(const catalog::Schema *schema,
                                  AbstractExpression *expr) {
    bool dummy;
    std::vector<const catalog::Schema *> schemas = {schema};
    TransformExpression(nullptr, nullptr, expr, schemas, dummy, false);
  }

  /**
   * This function walks an expression tree and fills in information about
   * columns and functions. Also generates a list of column ids we need to
   * fetch
   * from the base tile groups. Simultaneously generates a mapping of the
   * original column
   * id to the id in the logical tiles returned by the base tile groups
   *
   * This function is useful in determining information used by projection
   * plans
   */
  static void TransformExpression(std::vector<oid_t> &column_ids,
                                  AbstractExpression *expr,
                                  const catalog::Schema &schema,
                                  bool &needs_projection) {
    std::vector<std::unordered_map<oid_t, oid_t>> column_mapping = {
        std::unordered_map<oid_t, oid_t>()};
    std::vector<const catalog::Schema *> schemas = {&schema};
    TransformExpression(&column_mapping, &column_ids, expr, schemas,
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
      std::vector<std::unordered_map<oid_t, oid_t>> *column_mapping,
      std::vector<oid_t> *column_ids, AbstractExpression *expr,
      std::vector<const catalog::Schema *> &schemas, bool &needs_projection,
      bool find_columns) {
    if (expr == nullptr) {
      return;
    }
    size_t num_children = expr->GetChildrenSize();
    // do dfs to transform all children
    for (size_t child = 0; child < num_children; child++) {
      TransformExpression(column_mapping, column_ids,
                          expr->GetModifiableChild(child), schemas,
                          needs_projection, find_columns);
    }
    // if this is a column, we need to find if it is exists in the schema
    if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
        expr->GetValueType() == type::Type::INVALID) {
      auto val_expr = (expression::TupleValueExpression *)expr;
      oid_t col_id = -1;
      oid_t index = -1;
      catalog::Column column;
      for (int i = 0; i < (int)schemas.size(); i++) {
        auto &schema = schemas[i];
        col_id = schema->GetColumnID(val_expr->GetColumnName());
        if (col_id != (oid_t)-1) {
          index = i;
          column = schema->GetColumn(col_id);
          break;
        }
      }
      // exception if we can't find the requested column by name
      if (col_id == (oid_t)-1) {
        throw Exception("Column " + val_expr->GetColumnName() + " not found");
      }
      // make sure the column we need is returned from the scan
      // and we know where it is (for projection)
      size_t mapped_position;
      if (find_columns) {
        if ((*column_mapping)[index].count(col_id) == 0) {
          mapped_position = column_ids->size();
          column_ids->push_back(col_id);
          (*column_mapping)[index][col_id] = mapped_position;
        } else {
          mapped_position = (*column_mapping)[index][col_id];
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
    // if we have any expression besides column expressions and star, we
    // need to add a projection node
    else if (expr->GetExpressionType() != ExpressionType::STAR) {
      needs_projection = true;
    }
    // if the expression is a function, do a lookup and make sure it exists
    if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
      auto func_expr = (expression::FunctionExpression *)expr;
      auto catalog = catalog::Catalog::GetInstance();
      const catalog::FunctionData &func_data =
          catalog->GetFunction(func_expr->func_name_);
      func_expr->SetFunctionExpressionParameters(func_data.func_ptr_,
                                                 func_data.return_type_,
                                                 func_data.argument_types_);
    }
    // make sure the return types for expressions are set correctly
    // this is useful in operator expressions
    expr->DeduceExpressionType();
  }
};
}
}
