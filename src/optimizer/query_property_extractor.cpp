//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.cpp
//
// Identification: src/optimizer/query_property_extractor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/parser/statements.h>
#include "optimizer/query_property_extractor.h"

#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "optimizer/properties.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"
#include "storage/data_table.h"

using std::vector;
using std::shared_ptr;
using std::unique_ptr;
using std::move;

namespace peloton {
namespace optimizer {

PropertySet QueryPropertyExtractor::GetProperties(parser::SQLStatement *stmt) {
  stmt->Accept(this);
  return property_set_;
}

void QueryPropertyExtractor::Visit(const parser::SelectStatement *select_stmt) {
  // Generate PropertyPredicate
  auto predicate = select_stmt->where_clause;
  if (predicate != nullptr) {
    property_set_.AddProperty(shared_ptr<PropertyPredicate>(
        new PropertyPredicate(predicate->Copy())));
  }

  // Generate PropertyColumns
  vector<oid_t> column_ids;
  bool needs_projection = false;
  if ((*select_stmt->getSelectList())[0]->GetExpressionType() ==
      ExpressionType::STAR) {
    // TODO: Add support for combination of STAR expr and other cols expr.
    property_set_.AddProperty(
        shared_ptr<PropertyColumns>(new PropertyColumns(true)));
  } else {
    vector<expression::AbstractExpression *> column_exprs;
    ExprSet column_set;
    for (auto col : *select_stmt->select_list) {
      // Expression name is used in TrafficCop
      col->DeduceExpressionName();
      if (col->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        column_exprs.emplace_back(col);
      } else {
        needs_projection = true;
      }
      // Add all columns in select list to column_set
      expression::ExpressionUtil::GetTupleValueExprs(column_set, col);
    }
    // Add all the missing columns in Orderby
    if (select_stmt->order != nullptr) {
      for (auto expr : *select_stmt->order->exprs)
        expression::ExpressionUtil::GetTupleValueExprs(column_set, expr);
    }

    // Add PropertyDistinct
    if (select_stmt->select_distinct) {
      vector<expression::AbstractExpression *> distinct_column_exprs(
          column_exprs);
      property_set_.AddProperty(shared_ptr<PropertyDistinct>(
          new PropertyDistinct(move(distinct_column_exprs))));
    }
    // Add all the missing columns in GroupBy
    if (select_stmt->group_by != nullptr) {
      for (auto expr : *select_stmt->group_by->columns)
        expression::ExpressionUtil::GetTupleValueExprs(column_set, expr);
    }

    // If any missing column is added, we need a projection.
    if (column_set.size() > column_exprs.size()) {
      needs_projection = true;
      vector<expression::AbstractExpression *> columns(column_set.begin(),
                                                       column_set.end());
      property_set_.AddProperty(
          shared_ptr<PropertyColumns>(new PropertyColumns(move(columns))));
    } else {
      property_set_.AddProperty(
          shared_ptr<PropertyColumns>(new PropertyColumns(move(column_exprs))));
    }
  }

  // Generate PropertyProjection
  if (needs_projection) {
    vector<unique_ptr<expression::AbstractExpression>> output_expressions;
    for (auto col : *select_stmt->select_list) {
      output_expressions.push_back(
          unique_ptr<expression::AbstractExpression>(col->Copy()));
    }
    property_set_.AddProperty(shared_ptr<PropertyProjection>(
        new PropertyProjection(move(output_expressions))));
  }

  // TODO add PropertyDistinct
  if (select_stmt->select_distinct) {
    vector<expression::AbstractExpression *> distinct_column_exprs();
  }

  // Generate PropertySort
  if (select_stmt->order != nullptr) {
    select_stmt->order->Accept(this);
  }
};
void QueryPropertyExtractor::Visit(const parser::TableRef *) {}
void QueryPropertyExtractor::Visit(const parser::JoinDefinition *) {}
void QueryPropertyExtractor::Visit(const parser::GroupByDescription *) {}
void QueryPropertyExtractor::Visit(const parser::OrderDescription *node) {
  // TODO: Only support order by base table columns
  vector<bool> sort_ascendings;
  vector<expression::AbstractExpression *> sort_cols;
  auto len = node->exprs->size();
  for (size_t idx = 0; idx < len; idx++) {
    auto &expr = node->exprs->at(idx);
    sort_cols.emplace_back(expr);
    sort_ascendings.push_back(node->types->at(idx) == parser::kOrderAsc);
  }

  property_set_.AddProperty(
      shared_ptr<PropertySort>(new PropertySort(sort_cols, sort_ascendings)));
}
void QueryPropertyExtractor::Visit(const parser::LimitDescription *) {}

void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::InsertStatement *op) {}
void QueryPropertyExtractor::Visit(const parser::DeleteStatement *op) {
  if (op->expr != nullptr) {
    property_set_.AddProperty(
        shared_ptr<PropertyPredicate>(new PropertyPredicate(op->expr->Copy())));
  }
  property_set_.AddProperty(shared_ptr<PropertyColumns>(
      new PropertyColumns(vector<expression::AbstractExpression *>())));
}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::DropStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::PrepareStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::ExecuteStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::TransactionStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::UpdateStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::CopyStatement *op) {}

} /* namespace optimizer */
} /* namespace peloton */
