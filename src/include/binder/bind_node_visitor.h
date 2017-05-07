//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bind_node_visitor.h
//
// Identification: src/include/binder/binder_node_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "binder/binder_context.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace binder {

class BindNodeVisitor : public SqlNodeVisitor {
 public:
  BindNodeVisitor();

  void BindNameToNode(parser::SQLStatement *tree);
  void Visit(const parser::SelectStatement *) override;

  // Some sub query nodes inside SelectStatement
  void Visit(const parser::JoinDefinition *) override;
  void Visit(const parser::TableRef *) override;
  void Visit(const parser::GroupByDescription *) override;
  void Visit(const parser::OrderDescription *) override;
  void Visit(const parser::LimitDescription *) override;

  void Visit(const parser::CreateStatement *) override;
  void Visit(const parser::CreateFunctionStatement *) override;
  void Visit(const parser::InsertStatement *) override;
  void Visit(const parser::DeleteStatement *) override;
  void Visit(const parser::DropStatement *) override;
  void Visit(const parser::PrepareStatement *) override;
  void Visit(const parser::ExecuteStatement *) override;
  void Visit(const parser::TransactionStatement *) override;
  void Visit(const parser::UpdateStatement *) override;
  void Visit(const parser::CopyStatement *) override;

  //  void Visit(expression::ComparisonExpression* expr) override;
  //  void Visit(expression::AggregateExpression* expr) override;
  //  void Visit(expression::ConjunctionExpression* expr) override;
  //  void Visit(expression::FunctionExpression* expr) override;
  //  void Visit(expression::OperatorExpression* expr) override;
  //  void Visit(expression::ParameterValueExpression* expr) override;
  //  void Visit(expression::StarExpression* expr) override;
  void Visit(expression::ConstantValueExpression* expr) override;
  void Visit(expression::TupleValueExpression *expr) override;

 private:
  std::shared_ptr<BinderContext> context_;
};

}  // binder
}  // peloton
