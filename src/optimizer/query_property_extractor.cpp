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
#include "parser/sql_statement.h"

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

void QueryPropertyExtractor::Visit(
    UNUSED_ATTRIBUTE const parser::SelectStatement *){};

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
    UNUSED_ATTRIBUTE const parser::ImportStatement *op) {}

} /* namespace optimizer */
} /* namespace peloton */
