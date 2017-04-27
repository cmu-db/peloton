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
#include "storage/data_table.h"
#include "expression/tuple_value_expression.h"
#include "type/value_factory.h"
#include "expression/parameter_value_expression.h"
#include "expression/constant_value_expression.h"
#include "catalog/schema.h"

namespace peloton {
namespace optimizer {
namespace util {
/**
 * This function checks whether the current expression can enable index
 * scan for the statement. If it is index searchable, returns true and
 * set the corresponding data structures that will be used in creating
 * index scan node. Otherwise, returns false.
 */
bool CheckIndexSearchable(
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
      int max_columns = 0;
      int index_index = 0;
      for (auto& column_set : target_table->GetIndexColumns()) {
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
 * This function replaces all COLUMN_REF expressions with TupleValue
 * expressions
 */
void GetPredicateColumns(
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

} /* namespace util */
} /* namespace optimizer */
} /* namespace peloton */
