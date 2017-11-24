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

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "type/type_id.h"

#include "expression/aggregate_expression.h"
#include "expression/case_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/star_expression.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace binder {

BindNodeVisitor::BindNodeVisitor(concurrency::Transaction *txn,
                                 std::string default_database_name)
    : txn_(txn), default_database_name_(default_database_name) {
  context_ = nullptr;
}

void BindNodeVisitor::BindNameToNode(parser::SQLStatement *tree) {
  tree->Accept(this);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node) {
  // Save the upper level context
  auto pre_context = context_;
  context_ = std::make_shared<BinderContext>();
  context_->upper_context = pre_context;
  if (node->from_table != nullptr) {
    node->from_table->Accept(this);
  }

  if (node->where_clause != nullptr) node->where_clause->Accept(this);
  if (node->order != nullptr) node->order->Accept(this);
  if (node->limit != nullptr) node->limit->Accept(this);
  if (node->group_by != nullptr) node->group_by->Accept(this);
  for (auto &select_element : node->select_list) {
    select_element->Accept(this);
  }

  // Restore the upper level context
  context_ = context_->upper_context;
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
  if (node->select != nullptr) node->select->Accept(this);
  // Join
  else if (node->join != nullptr)
    node->join->Accept(this);
  // Multiple tables
  else if (!node->list.empty()) {
    for (auto &table : node->list) table->Accept(this);
  }
  // Single table
  else {
    context_->AddTable(node, default_database_name_, txn_);
  }
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node) {
  for (auto &col : node->columns) {
    col->Accept(this);
  }
  if (node->having != nullptr) node->having->Accept(this);
}
void BindNodeVisitor::Visit(parser::OrderDescription *node) {
  for (auto &expr : node->exprs)
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
  context_->AddTable(node->GetDatabaseName(), node->GetTableName(), txn_);

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
    std::tuple<oid_t, oid_t> table_id_tuple;

    std::string table_name = expr->GetTableName();
    std::string col_name = expr->GetColumnName();

    // Convert all the names to lower cases
    std::transform(table_name.begin(), table_name.end(), table_name.begin(),
                   ::tolower);
    std::transform(col_name.begin(), col_name.end(), col_name.begin(),
                   ::tolower);

    type::TypeId value_type;
    // Table name not specified in the expression
    if (table_name.empty()) {
      if (!BinderContext::GetColumnPosTuple(context_, col_name, col_pos_tuple,
                                            table_name, value_type, txn_))
        throw BinderException("Cannot find column " + col_name);
      expr->SetTableName(table_name);
    }
    // Table name is present
    else {
      // Find the corresponding table in the context
      if (!BinderContext::GetTableIdTuple(context_, table_name,
                                          &table_id_tuple))
        throw BinderException("Invalid table reference " +
                              expr->GetTableName());
      // Find the column offset in that table
      if (!BinderContext::GetColumnPosTuple(col_name, table_id_tuple,
                                            col_pos_tuple, value_type, txn_))
        throw BinderException("Cannot find column " + col_name);
    }
    expr->SetValueType(value_type);
    expr->SetBoundOid(col_pos_tuple);
  }
}

void BindNodeVisitor::Visit(expression::CaseExpression *expr) {
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCond(i)->Accept(this);
  }
}

void BindNodeVisitor::Visit(expression::StarExpression *expr) {
  if (!BinderContext::HasTables(context_))
    throw BinderException("Invalid expression" + expr->GetInfo());
}

// Deduce value type for these expressions
void BindNodeVisitor::Visit(expression::OperatorExpression *expr) {
  SqlNodeVisitor::Visit(expr);
  expr->DeduceExpressionType();
}
void BindNodeVisitor::Visit(expression::AggregateExpression *expr) {
  SqlNodeVisitor::Visit(expr);
  expr->DeduceExpressionType();
}

void BindNodeVisitor::Visit(expression::FunctionExpression *expr) {
  // Visit the subtree first
  SqlNodeVisitor::Visit(expr);

  // Specialize the first argument (DatePartType) for date functions, otherwise
  // we have to do the string comparison to find out the corresponding
  // DatePartType when scanning every tuple.
  auto func_name = expr->GetFuncName();
  if (func_name == "date_trunc" or func_name == "extract") {
    // Check the type of the first argument. Should be VARCHAR
    auto date_part = expr->GetChild(0);
    if (date_part->GetValueType() != type::TypeId::VARCHAR) {
      throw Exception(EXCEPTION_TYPE_EXPRESSION,
                      "Incorrect argument type to function: " + func_name +
                          ". Argument 0 expected type VARCHAR but found " +
                          TypeIdToString(date_part->GetValueType()) + ".");
    }

    // Convert the first argument to DatePartType
    auto date_part_type = StringToDatePartType(
        date_part->Evaluate(nullptr, nullptr, nullptr).ToString());
    auto date_part_integer = type::ValueFactory::GetIntegerValue(
        static_cast<int32_t>(date_part_type));

    // Replace the first argument with an Integer expression of the DatePartType
    expr->SetChild(0,
                   new expression::ConstantValueExpression(date_part_integer));
  }

  // Check catalog and bind function
  std::vector<type::TypeId> argtypes;
  for (size_t i = 0; i < expr->GetChildrenSize(); i++)
    argtypes.push_back(expr->GetChild(i)->GetValueType());
  // Check and set the function ptr
  auto catalog = catalog::Catalog::GetInstance();
  const catalog::FunctionData &func_data =
      catalog->GetFunction(expr->GetFuncName(), argtypes);
  LOG_DEBUG("Function %s found in the catalog", func_data.func_name_.c_str());
  LOG_DEBUG("Argument num: %ld", func_data.argument_types_.size());
  expr->SetFunctionExpressionParameters(func_data.func_, func_data.return_type_,
                                        func_data.argument_types_);
}

}  // namespace binder
}  // namespace peloton
