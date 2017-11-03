//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bind_node_visitor.cpp
//
// Identification: src/binder/binder_node_visitor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "binder/bind_node_visitor.h"
#include "expression/star_expression.h"

#include "expression/case_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/subquery_expression.h"

namespace peloton {
namespace binder {

BindNodeVisitor::BindNodeVisitor(
    concurrency::Transaction *txn,
    std::string default_database_name)
    : txn_(txn),
      default_database_name_(default_database_name) {
  context_ = nullptr;
}

void BindNodeVisitor::BindNameToNode(parser::SQLStatement *tree) {
  tree->Accept(this);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node) {
  context_ = std::make_shared<BinderContext>();
  // Upper context should be set outside (e.g. when where contains subquery)
  //  context_->SetUpperContext(pre_context);
  if (node->from_table != nullptr) node->from_table->Accept(this);
  if (node->where_clause != nullptr) node->where_clause->Accept(this);
  if (node->order != nullptr) node->order->Accept(this);
  if (node->limit != nullptr) node->limit->Accept(this);
  if (node->group_by != nullptr) node->group_by->Accept(this);

  std::vector<std::unique_ptr<expression::AbstractExpression>> new_select_list;
  for (auto &select_element : node->select_list) {
    if (select_element->GetExpressionType() == ExpressionType::STAR) {
      context_->GenerateAllColumnExpressions(new_select_list);
      continue;
    }

    select_element->Accept(this);

    // Recursively deduce expression value type
    expression::ExpressionUtil::EvaluateExpression({ExprMap()},
                                                   select_element.get());
    // Recursively deduce expression name
    select_element->DeduceExpressionName();
    new_select_list.push_back(std::move(select_element));
  }
  node->select_list = std::move(new_select_list);
}

// Some sub query nodes inside SelectStatement
void BindNodeVisitor::Visit(parser::JoinDefinition *node) {
  // The columns in join condition can only bind to the join tables
  node->left->Accept(this);
  node->right->Accept(this);
  node->condition->Accept(this);
}

void BindNodeVisitor::Visit(parser::TableRef *node) {
  // Nested select. Not supported in the current executors
  if (node->select != nullptr) {
    if (node->alias.empty())
      throw Exception("Alias not found for query derived table");

    // Save the previous context
    auto pre_context = context_;
    node->select->Accept(this);
    // Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    context_->AddNestedTable(node->alias, node->select->select_list);
  }
  // Join
  else if (node->join != nullptr)
    node->join->Accept(this);
  // Multiple tables
  else if (!node->list.empty()) {
    for (auto &table : node->list) table->Accept(this);
  }
  // Single table
  else {
    context_->AddRegularTable(node, default_database_name_, txn_);
  }
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node) {
  for (auto& col : node->columns) {
    col->Accept(this);
  }
  if (node->having != nullptr) node->having->Accept(this);
}
void BindNodeVisitor::Visit(parser::OrderDescription *node) {
  for (auto& expr : node->exprs)
    if (expr != nullptr) expr->Accept(this);
}

void BindNodeVisitor::Visit(parser::UpdateStatement *node) {
  context_ = std::make_shared<BinderContext>();

  node->table->Accept(this);
  if (node->where != nullptr) node->where->Accept(this);
  for (auto &update : node->updates) {
    update->value->Accept(this);
  }

  // TODO: Update columns are not bound because they are char*
  // not TupleValueExpression in update_statement.h

  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::DeleteStatement *node) {
  context_ = std::make_shared<BinderContext>();
  node->TryBindDatabaseName(default_database_name_);
  context_->AddRegularTable(node->GetDatabaseName(), node->GetTableName(),
                            node->GetTableName(), txn_);

  if (node->expr != nullptr) node->expr->Accept(this);

  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::LimitDescription *) {}
void BindNodeVisitor::Visit(parser::CopyStatement *) {}
void BindNodeVisitor::Visit(parser::CreateStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
}
void BindNodeVisitor::Visit(parser::InsertStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
  if (node->select != nullptr) node->select->Accept(this);
  context_ = nullptr;
}
void BindNodeVisitor::Visit(parser::DropStatement *) {}
void BindNodeVisitor::Visit(parser::PrepareStatement *) {}
void BindNodeVisitor::Visit(parser::ExecuteStatement *) {}
void BindNodeVisitor::Visit(parser::TransactionStatement *) {}
void BindNodeVisitor::Visit(parser::AnalyzeStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
}

// void BindNodeVisitor::Visit(const parser::ConstantValueExpression *) {}

void BindNodeVisitor::Visit(expression::TupleValueExpression *expr) {
  if (!expr->GetIsBound()) {
    std::tuple<oid_t, oid_t, oid_t> col_pos_tuple;
    std::shared_ptr<catalog::TableCatalogObject> table_obj = nullptr;

    std::string table_name = expr->GetTableName();
    std::string col_name = expr->GetColumnName();

    // Convert all the names to lower cases
    std::transform(table_name.begin(), table_name.end(), table_name.begin(),
                   ::tolower);
    std::transform(col_name.begin(), col_name.end(), col_name.begin(),
                   ::tolower);

    type::TypeId value_type;
    // Table name not specified in the expression. Loop through all the table
    // in the binder context.
    if (table_name.empty()) {
      if (!BinderContext::GetColumnPosTuple(context_, col_name, col_pos_tuple,
                                            table_name, value_type)) {
        throw Exception("Cannot find column " + col_name);
      }
      expr->SetTableName(table_name);
    }
    // Table name is present
    else {
      // Regular table
      if (BinderContext::GetRegularTableObj(context_, table_name, table_obj)) {
        if (!BinderContext::GetColumnPosTuple(col_name, table_obj,
                                              col_pos_tuple, value_type)) {
          throw Exception("Cannot find column " + col_name);
        }
      }
      // Nested table
      else if (!BinderContext::CheckNestedTableColumn(context_, table_name,
                                                      col_name, value_type))
        throw Exception("Invalid table reference " + expr->GetTableName());
    }
    expr->SetColName(col_name);
    expr->SetValueType(value_type);
    expr->SetBoundOid(col_pos_tuple);
  }
}

void BindNodeVisitor::Visit(expression::CaseExpression *expr) {
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCond(i)->Accept(this);
  }
}

void BindNodeVisitor::Visit(expression::SubqueryExpression *expr) {
  LOG_INFO("Bind subquery: context switch...");
  context_ = std::make_shared<BinderContext>(context_);
  PL_ASSERT(context_->GetUpperContext() != nullptr);

  expr->GetSubSelect()->Accept(this);

  LOG_INFO("Bind subquery: context restore...");
  context_ = context_->GetUpperContext();
}

}  // namespace binder
}  // namespace peloton
