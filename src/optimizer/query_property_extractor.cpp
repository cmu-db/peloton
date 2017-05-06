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
  vector<shared_ptr<expression::AbstractExpression>> output_expressions;
  for (auto col : *select_stmt->select_list) {
    // Recursively deduce expression value type
    expression::ExpressionUtil::EvaluateExpression({ExprMap()}, col);
    // Recursively deduce expression name
    col->DeduceExpressionName();
    output_expressions.emplace_back(col->Copy());
  }
  property_set_.AddProperty(
      shared_ptr<PropertyColumns>(new PropertyColumns(output_expressions)));

  // Generate PropertyDistinct
  if (select_stmt->select_distinct) {
    auto distinct_column_exprs = output_expressions;
    property_set_.AddProperty(shared_ptr<PropertyDistinct>(
        new PropertyDistinct(move(distinct_column_exprs))));
  }

  // Generate PropertySort
  if (select_stmt->order != nullptr)
    select_stmt->order->Accept(this);
  
  // Generate PropertyLimit
  if (select_stmt->limit != nullptr)
    select_stmt->limit->Accept(this);
};
void QueryPropertyExtractor::Visit(const parser::TableRef *) {}
void QueryPropertyExtractor::Visit(const parser::JoinDefinition *) {}
void QueryPropertyExtractor::Visit(const parser::GroupByDescription *) {}
void QueryPropertyExtractor::Visit(const parser::OrderDescription *node) {
  vector<bool> sort_ascendings;
  vector<shared_ptr<expression::AbstractExpression>> sort_cols;
  auto len = node->exprs->size();
  for (size_t idx = 0; idx < len; idx++) {
    auto &expr = node->exprs->at(idx);
    sort_cols.emplace_back(expr->Copy());
    sort_ascendings.push_back(node->types->at(idx) == parser::kOrderAsc);
  }

  property_set_.AddProperty(shared_ptr<PropertySort>(
      new PropertySort(move(sort_cols), sort_ascendings)));
}
void QueryPropertyExtractor::Visit(const parser::LimitDescription *limit) {
  // When offset is not specified in the query, parser will set offset to -1
  int64_t offset = limit->offset == -1 ? 0 : limit->offset;
  property_set_.AddProperty(shared_ptr<PropertyLimit>(
      new PropertyLimit(offset, limit->limit)));
}

void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::InsertStatement *op) {}
void QueryPropertyExtractor::Visit(const parser::DeleteStatement *op) {
  if (op->expr != nullptr) {
    property_set_.AddProperty(
        shared_ptr<PropertyPredicate>(new PropertyPredicate(op->expr->Copy())));
  }
  property_set_.AddProperty(shared_ptr<PropertyColumns>(new PropertyColumns(
      vector<shared_ptr<expression::AbstractExpression>>())));
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
    UNUSED_ATTRIBUTE const parser::UpdateStatement *op) {
  if (op->where != nullptr) {
    property_set_.AddProperty(
        shared_ptr<PropertyPredicate>(new PropertyPredicate(op->where->Copy())));
  }
  property_set_.AddProperty(shared_ptr<PropertyColumns>(new PropertyColumns(
      vector<shared_ptr<expression::AbstractExpression>>())));
}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::CopyStatement *op) {}

} /* namespace optimizer */
} /* namespace peloton */
