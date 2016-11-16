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
#include "optimizer/properties.h"
#include "parser/sql_statement.h"
#include "parser/statement_select.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {

PropertySet QueryPropertyExtractor::GetProperties(parser::SQLStatement *stmt) {
  stmt->Accept(this);
  return property_set_;
}

void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Variable *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Constant *){};
void QueryPropertyExtractor::visit(
    UNUSED_ATTRIBUTE const OperatorExpression *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const AndOperator *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const OrOperator *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const NotOperator *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Attribute *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Table *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Join *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const OrderBy *){};
void QueryPropertyExtractor::visit(UNUSED_ATTRIBUTE const Select *){};

void QueryPropertyExtractor::Visit(const parser::SelectStatement *stmt) {
  std::vector<Column *> columns;

  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          stmt->from_table->GetDatabaseName(),
          stmt->from_table->GetTableName());

  catalog::Schema *schema = target_table->GetSchema();
  oid_t table_oid = target_table->GetOid();
  for (oid_t col_id = 0; col_id < schema->GetColumnCount(); col_id++) {
    catalog::Column schema_col = schema->GetColumn(col_id);
    Column *col = manager_.LookupColumn(table_oid, col_id);
    if (col == nullptr) {
      col = manager_.AddBaseColumn(schema_col.GetType(), schema_col.GetLength(),
                                   schema_col.GetName(), schema_col.IsInlined(),
                                   table_oid, col_id);
    }
    columns.push_back(col);
  }

  property_set_.AddProperty(new PropertyColumns(std::move(columns)));
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
