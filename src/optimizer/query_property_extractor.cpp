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

#include "optimizer/query_property_extractor.h"

#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "optimizer/properties.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {

PropertySet QueryPropertyExtractor::GetProperties(parser::SQLStatement *stmt) {
  stmt->Accept(this);
  return property_set_;
}

void QueryPropertyExtractor::GetColumnsFromSelecrElements(expression::AbstractExpression* expr,
                                                          std::vector<Column *>* columns,
                                                          bool& needs_projection) {
  if (expr == nullptr) {
    return;
  }

  size_t num_children = expr->GetChildrenSize();
  for (size_t child = 0; child < num_children; child++)
    GetColumnsFromSelecrElements(expr->GetModifiableChild(child), columns, needs_projection);

  if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
      expr->GetValueType() == type::Type::INVALID) {
    auto col = manager_.BindColumnRefToColumn((expression::TupleValueExpression *)expr);
    if (col != nullptr)
      columns->push_back(col);
  }
  else if (expr->GetExpressionType() != ExpressionType::STAR) {
    needs_projection = true;
  }

}

// Extract the properties that are required in the final output.
void QueryPropertyExtractor::Visit(const parser::SelectStatement *select_stmt) {
  std::vector<Column *> columns = {};

  if (select_stmt->from_table != nullptr) {
    select_stmt->from_table->Accept(this);
  }

  // Add predicate property
  auto predicate = select_stmt->where_clause;
  if (predicate != nullptr) {
    property_set_.AddProperty(std::shared_ptr<PropertyPredicate>(
        new PropertyPredicate(predicate->Copy())));
  }

  std::vector<oid_t> column_ids;
  bool needs_projection = false;

  // Transform output expressions
  for (auto col : *select_stmt->select_list) {
    GetColumnsFromSelecrElements(col, &columns, needs_projection);
  }


  property_set_.AddProperty(
      std::shared_ptr<PropertyColumns>(new PropertyColumns(columns)));

  if (needs_projection) {
    auto output_expressions =
        std::vector<std::unique_ptr<expression::AbstractExpression> >();
    // Add output expressions property
    for (auto col : *select_stmt->select_list) {
      output_expressions.push_back(
          std::unique_ptr<expression::AbstractExpression>(col->Copy()));
    }
    property_set_.AddProperty(std::shared_ptr<PropertyProjection>(
        new PropertyProjection(std::move(output_expressions))));
  }

  if (select_stmt->order!=nullptr)
    select_stmt->order->Accept(this);
};

void QueryPropertyExtractor::Visit(const parser::TableRef* ) {}
void QueryPropertyExtractor::Visit(const parser::JoinDefinition* ) {}
void QueryPropertyExtractor::Visit(const parser::GroupByDescription *) {}
void QueryPropertyExtractor::Visit(const parser::OrderDescription *) {}
void QueryPropertyExtractor::Visit(const parser::LimitDescription *) {}

void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::InsertStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::DeleteStatement *op) {}
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
