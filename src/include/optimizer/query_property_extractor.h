//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.cpp
//
// Identification: src/include/optimizer/query_property_extractor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/schema.h"
#include "common/sql_node_visitor.h"
#include "optimizer/column_manager.h"
#include "optimizer/property_set.h"

namespace peloton {

namespace parser {
class SQLStatement;
}

namespace optimizer {

/*
 * Extract physical properties from the parsed query.
 * Physical properties are those fields that can be directly added to the plan,
 * and don't need to perform transformations on.
 */
class QueryPropertyExtractor : public SqlNodeVisitor {
 public:
  PropertySet GetProperties(parser::SQLStatement *tree);

  // We only assume the statement is selecting from one table for now
  void Visit(parser::SelectStatement *) override;

  void Visit(parser::TableRef *) override;
  void Visit(parser::JoinDefinition *) override;
  void Visit(parser::GroupByDescription *) override;
  void Visit(parser::OrderDescription *) override;
  void Visit(parser::LimitDescription *) override;

  void Visit(parser::CreateStatement *) override;
  void Visit(parser::InsertStatement *) override;
  void Visit(parser::DeleteStatement *) override;
  void Visit(parser::DropStatement *) override;
  void Visit(parser::PrepareStatement *) override;
  void Visit(parser::ExecuteStatement *) override;
  void Visit(parser::TransactionStatement *) override;
  void Visit(parser::UpdateStatement *) override;
  void Visit(parser::CopyStatement *) override;
  void Visit(parser::AnalyzeStatement *) override;

 private:
  // Required properties by the visitor
  PropertySet property_set_;
  vector<AnnotatedExpression> predicates_;
};

}  // namespace optimizer
}  // namespace peloton
