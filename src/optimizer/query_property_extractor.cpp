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

namespace peloton {
namespace optimizer {

PropertySet QueryPropertyExtractor::GetProperties(parser::SQLStatement *stmt) {
  stmt->Accept(this);
  return property_set_;
}

void QueryPropertyExtractor::Visit(const parser::SelectStatement *select_stmt) {
  // Get table pointer, id, and schema.
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          select_stmt->from_table->GetDatabaseName(),
          select_stmt->from_table->GetTableName());
  auto &schema = *target_table->GetSchema();

  // Add predicate property
  auto predicate = select_stmt->where_clause;
  if (predicate != nullptr) {
    expression::ExpressionUtil::TransformExpression(&schema, predicate);
    property_set_.AddProperty(std::shared_ptr<PropertyPredicate>(
        new PropertyPredicate(predicate->Copy())));
  }

  std::vector<oid_t> column_ids;
  bool needs_projection = false;

  // TODO: Support combination of STAR expr and other cols expr.
  // Only support single STAR expression
  if ((*select_stmt->getSelectList())[0]->GetExpressionType() ==
      ExpressionType::STAR) {
    property_set_.AddProperty(
        std::shared_ptr<PropertyColumns>(new PropertyColumns(true)));
  }
  else {
    std::vector<expression::TupleValueExpression *> column_exprs;

    // Transform output expressions
    for (auto col : *select_stmt->select_list) {
      expression::ExpressionUtil::TransformExpression(column_ids, col, schema,
                                                      needs_projection);
      if (col->GetExpressionType() == ExpressionType::VALUE_TUPLE)
        column_exprs.emplace_back(
            reinterpret_cast<expression::TupleValueExpression *>(col));
    }
    property_set_.AddProperty(
        std::shared_ptr<PropertyColumns>(new PropertyColumns(column_exprs)));
  }

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

  if (select_stmt->order != nullptr) {
    select_stmt->order->Accept(this);
  }
};
void QueryPropertyExtractor::Visit(const parser::TableRef *) {}
void QueryPropertyExtractor::Visit(const parser::JoinDefinition *) {}
void QueryPropertyExtractor::Visit(const parser::GroupByDescription *) {}
void QueryPropertyExtractor::Visit(const parser::OrderDescription *node) {
  // TODO: the parser node only support order by one column
  bool sort_ascending = false;
  if (node->type == parser::kOrderAsc) sort_ascending = true;

  auto sort_column_expr = (expression::TupleValueExpression *)node->expr;
  property_set_.AddProperty(std::shared_ptr<PropertySort>(
      new PropertySort({sort_column_expr}, {sort_ascending})));
}
void QueryPropertyExtractor::Visit(const parser::LimitDescription *) {}

void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::CreateStatement *op) {}
void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::InsertStatement *op) {}
void QueryPropertyExtractor::Visit(
    const parser::DeleteStatement *op) {
  if (op->expr != nullptr) {  
    property_set_.AddProperty(std::shared_ptr<PropertyPredicate>(
        new PropertyPredicate(op->expr->Copy())));
  }
  property_set_.AddProperty(
      std::shared_ptr<PropertyColumns>(
          new PropertyColumns(std::vector<expression::TupleValueExpression *>())));

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
