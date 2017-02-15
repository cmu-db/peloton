//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_node_visitor.h
//
// Identification: src/include/binder/binder_node_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/parser/select_statement.h>
#include "binder/bind_node_visitor.h"

namespace peloton {
namespace binder {

  BindNodeVisitor::BindNodeVisitor() {
    context_ = nullptr;
  }
  
  void BindNodeVisitor::BindNameToNode(parser::SQLStatement *tree) {
    tree->Accept(this);
  }
  

  void BindNodeVisitor::Visit(const parser::SelectStatement * node) {
    // Save the upper level context
    context_->upper_context = context_;

    context_ = std::make_shared(new BinderContext());
    if (node->from_table != nullptr)
      node->from_table->Accept(this);
    if (node->where_clause != nullptr)
      node->where_clause->Accept(this);
    if (node->order != nullptr)
      node->order->Accept(this);
    if (node->limit != nullptr)
      node->limit->Accept(this);
    if (node->group_by != nullptr)
      node->group_by->Accept(this);

    // Restore the upper level context
    context_ = context_->upper_context;

  }

  // Some sub query nodes inside SelectStatement
  void BindNodeVisitor::Visit(const parser::JoinDefinition *node) {
    node->left->Accept(this);
    node->right->Accept(this);
  }
  void BindNodeVisitor::Visit(const parser::TableRef * node) {
    // Nested select. Not supported in the current executors
    if (node->select != nullptr)
      node->select->Accept(this);
      // Join
    else if (node->join != nullptr)
      node->join->Accept(this);
      // Multiple tables
    else if (node->list != nullptr) {
      for (parser::TableRef* table:*(node->list))
        table->Accept(this);
    }
      // Single table
    else {
      context_->AddTable(node);
    }

  }
  void BindNodeVisitor::Visit(const parser::GroupByDescription *node) {
    for (auto col : *(node->columns)) {
      col->Accept(this);
    }
    if (node->having != nullptr)
      node->having->Accept(this);
  }
  void BindNodeVisitor::Visit(const parser::OrderDescription *node) {
    if (node->expr != nullptr)
      node->expr->Accept(this);
  }
  void BindNodeVisitor::Visit(const parser::LimitDescription *) {}

  void BindNodeVisitor::Visit(const parser::CreateStatement *) {}
  void BindNodeVisitor::Visit(const parser::InsertStatement *) {}
  void BindNodeVisitor::Visit(const parser::DeleteStatement *) {}
  void BindNodeVisitor::Visit(const parser::DropStatement *) {}
  void BindNodeVisitor::Visit(const parser::PrepareStatement *) {}
  void BindNodeVisitor::Visit(const parser::ExecuteStatement *) {}
  void BindNodeVisitor::Visit(const parser::TransactionStatement *) {}
  void BindNodeVisitor::Visit(const parser::UpdateStatement *) {}
  void BindNodeVisitor::Visit(const parser::CopyStatement *) {}

//  void BindNodeVisitor::Visit(expression::ComparisonExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::AggregateExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::ConjunctionExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::ConstantValueExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::FunctionExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::OperatorExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::ParameterValueExpression* expr) {}
//  void BindNodeVisitor::Visit(expression::StarExpression* expr) {}
  void BindNodeVisitor::Visit(expression::TupleValueExpression* expr) {
    if (!expr->IsObjectSet) {
      std::tuple<oid_t, oid_t> id_tuple_ptr;
      if (!context_->GetTableIdTuple(expr->GetTableName(), &id_tuple_ptr))
        throw Exception("Invalid name "+expr->GetTableName());
      storage::DataTable *target_table =
          catalog::Catalog::GetInstance()->GetTableWithName(
              expr->GetDatabaseName(),
              expr->GetTableName());
      expr->SetBoundObjectId()
    }
  }


} // binder
} // peloton