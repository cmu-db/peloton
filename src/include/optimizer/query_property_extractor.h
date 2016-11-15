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

#include "optimizer/column_manager.h"
#include "optimizer/property_set.h"
#include "optimizer/query_node_visitor.h"

namespace peloton {

namespace parser {
class SQLStatement;
}

namespace optimizer {

class QueryPropertyExtractor : public QueryNodeVisitor {
 public:
  QueryPropertyExtractor(ColumnManager &manager) : manager_(manager) {}

  PropertySet GetProperties(parser::SQLStatement *tree);

  // TODO: They're left here for compilation. Delete them later.
  void visit(const Variable *) override;
  void visit(const Constant *) override;
  void visit(const OperatorExpression *) override;
  void visit(const AndOperator *) override;
  void visit(const OrOperator *) override;
  void visit(const NotOperator *) override;
  void visit(const Attribute *) override;
  void visit(const Table *) override;
  void visit(const Join *) override;
  void visit(const OrderBy *) override;
  void visit(const Select *) override;

  void Visit(const parser::SelectStatement *) override;
  void Visit(const parser::CreateStatement *) override;
  void Visit(const parser::InsertStatement *) override;
  void Visit(const parser::DeleteStatement *) override;
  void Visit(const parser::DropStatement *) override;
  void Visit(const parser::PrepareStatement *) override;
  void Visit(const parser::ExecuteStatement *) override;
  void Visit(const parser::TransactionStatement *) override;
  void Visit(const parser::UpdateStatement *) override;
  void Visit(const parser::CopyStatement *) override;

 private:
  ColumnManager &manager_;

  PropertySet property_set_;
};

} /* namespace optimizer */
} /* namespace peloton */
