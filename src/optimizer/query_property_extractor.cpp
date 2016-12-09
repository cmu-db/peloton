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

#include "../include/parser/select_statement.h"
#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "optimizer/properties.h"
#include "parser/sql_statement.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {

PropertySet QueryPropertyExtractor::GetProperties(parser::SQLStatement *stmt) {
  stmt->Accept(this);
  return property_set_;
}

void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Table *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Join *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const OrderBy *){};

void QueryPropertyExtractor::Visit(const parser::SelectStatement *select_stmt) {
  std::vector<Column *> columns;

  // Get table pointer, id, and schema.
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          select_stmt->from_table->GetDatabaseName(),
          select_stmt->from_table->GetTableName());
  auto &schema = *target_table->GetSchema();

  // Add predicate property
  auto predicate = select_stmt->where_clause;
  expression::ExpressionUtil::TransformExpression(&schema, predicate);
  property_set_.AddProperty(std::shared_ptr<PropertyPredicate>(
      new PropertyPredicate(predicate->Copy())));

  // Add output expressions property
  auto output_expressions =
      std::vector<std::unique_ptr<expression::AbstractExpression> >();
  for (auto col : *select_stmt->select_list) {
    expression::ExpressionUtil::TransformExpression(&schema, col);
    output_expressions.push_back(
        std::unique_ptr<expression::AbstractExpression>(col->Copy()));
  }

  property_set_.AddProperty(std::shared_ptr<PropertyOutputExpressions>(
      new PropertyOutputExpressions(std::move(output_expressions))));

  /*
      // Traverse the select list to get the columns required by the result
      std::unordered_map<oid_t, oid_t> column_mapping;
      std::vector<oid_t> column_ids;
      bool needs_projection = false;
     for (oid_t col_id : column_ids) {
      catalog::Column schema_col = schema.GetColumn(col_id);
      Column *col = manager_.LookupColumn(table_oid, col_id);
      if (col == nullptr) {
        col = manager_.AddBaseColumn(schema_col.GetType(),
    schema_col.GetLength(),
                                     schema_col.GetName(),
    schema_col.IsInlined(),
                                     table_oid, col_id);
      }
      columns.push_back(col);
    }*/
};

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
