//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bind_node_visitor.cpp
//
// Identification: src/binder/bind_node_visitor.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "binder/bind_node_visitor.h"
#include "expression/star_expression.h"
#include "catalog/catalog.h"
#include "type/type_id.h"

#include "expression/aggregate_expression.h"
#include "expression/case_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/star_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/subquery_expression.h"

namespace peloton {
namespace binder {

BindNodeVisitor::BindNodeVisitor(concurrency::TransactionContext *txn,
                                 std::string default_database_name)
    : txn_(txn), default_database_name_(default_database_name) {
  catalog_ = catalog::Catalog::GetInstance();
  context_ = nullptr;
}

void BindNodeVisitor::BindNameToNode(parser::SQLStatement *tree) {
  tree->Accept(this);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node) {
  context_ = std::make_shared<BinderContext>(context_);
  if (node->from_table != nullptr) {
    node->from_table->Accept(this);
  }
  if (node->where_clause != nullptr) {
    node->where_clause->Accept(this);
    // Derive depth for all exprs in the where clause
    node->where_clause->DeriveDepth();
    node->where_clause->DeriveSubqueryFlag();
  }
  if (node->order != nullptr) {
    node->order->Accept(this);
  }
  if (node->limit != nullptr) {
    node->limit->Accept(this);
  }
  if (node->group_by != nullptr) {
    node->group_by->Accept(this);
  }

  std::vector<std::unique_ptr<expression::AbstractExpression>> new_select_list;
  for (auto &select_element : node->select_list) {
    if (select_element->GetExpressionType() == ExpressionType::STAR) {
      context_->GenerateAllColumnExpressions(new_select_list);
      continue;
    }

    select_element->Accept(this);
    // Derive depth for all exprs in the select clause
    if (select_element->GetExpressionType() == ExpressionType::STAR) {
      select_element->SetDepth(context_->GetDepth());
    } else {
      select_element->DeriveDepth();
    }
    select_element->DeriveSubqueryFlag();

    // Recursively deduce expression value type
    expression::ExpressionUtil::EvaluateExpression({ExprMap()},
                                                   select_element.get());
    // Recursively deduce expression name
    select_element->DeduceExpressionName();
    new_select_list.push_back(std::move(select_element));
  }
  node->select_list = std::move(new_select_list);
  // Temporarily discard const to update the depth
  const_cast<parser::SelectStatement *>(node)->depth = context_->GetDepth();
  context_ = context_->GetUpperContext();
}

// Some sub query nodes inside SelectStatement
void BindNodeVisitor::Visit(parser::JoinDefinition *node) {
  // The columns in join condition can only bind to the join tables
  node->left->Accept(this);
  node->right->Accept(this);
  node->condition->Accept(this);
}

void BindNodeVisitor::Visit(parser::TableRef *node) {
  if (node->select != nullptr) {
    if (node->alias.empty()) {
      throw Exception("Alias not found for query derived table");
    }

    // Save the previous context
    auto pre_context = context_;
    node->select->Accept(this);
    // Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    context_->AddNestedTable(node->alias, node->select->select_list);
  }
  // Join
  else if (node->join != nullptr) {
    node->join->Accept(this);
    // Multiple tables
  } else if (!node->list.empty()) {
    for (auto &table : node->list) table->Accept(this);
  }
  // Single table
  else {
    context_->AddRegularTable(node, default_database_name_, txn_);
  }
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node) {
  for (auto &col : node->columns) {
    col->Accept(this);
  }
  if (node->having != nullptr) {
    node->having->Accept(this);
  }
}
void BindNodeVisitor::Visit(parser::OrderDescription *node) {
  for (auto &expr : node->exprs) {
    if (expr != nullptr) {
      expr->Accept(this);
    }
  }
}

void BindNodeVisitor::Visit(parser::UpdateStatement *node) {
  context_ = std::make_shared<BinderContext>(nullptr);

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
  context_ = std::make_shared<BinderContext>(nullptr);
  node->TryBindDatabaseName(default_database_name_);
  context_->AddRegularTable(node->GetDatabaseName(), node->GetTableName(),
                            node->GetTableName(), txn_);

  if (node->expr != nullptr) {
    node->expr->Accept(this);
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::LimitDescription *) {}
void BindNodeVisitor::Visit(parser::CopyStatement *) {}
void BindNodeVisitor::Visit(parser::CreateFunctionStatement *) {}
void BindNodeVisitor::Visit(parser::CreateStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
}
void BindNodeVisitor::Visit(parser::InsertStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
  if (node->select != nullptr) {
    node->select->Accept(this);
  }
  context_ = nullptr;
}
void BindNodeVisitor::Visit(parser::DropStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
}
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
    type::TypeId value_type;
    int depth = -1;

    std::string table_name = expr->GetTableName();
    std::string col_name = expr->GetColumnName();

    // Convert all the names to lower cases
    std::transform(table_name.begin(), table_name.end(), table_name.begin(),
                   ::tolower);
    std::transform(col_name.begin(), col_name.end(), col_name.begin(),
                   ::tolower);

    // Table name not specified in the expression. Loop through all the table
    // in the binder context.
    if (table_name.empty()) {
      if (!BinderContext::GetColumnPosTuple(context_, col_name, col_pos_tuple,
                                            table_name, value_type, depth)) {
        throw Exception("Cannot find column " + col_name);
      }
      expr->SetTableName(table_name);
    }
    // Table name is present
    else {
      // Regular table
      if (BinderContext::GetRegularTableObj(context_, table_name, table_obj,
                                            depth)) {
        if (!BinderContext::GetColumnPosTuple(col_name, table_obj,
                                              col_pos_tuple, value_type)) {
          throw Exception("Cannot find column " + col_name);
        }
      }
      // Nested table
      else if (!BinderContext::CheckNestedTableColumn(
                   context_, table_name, col_name, value_type, depth))
        throw Exception("Invalid table reference " + expr->GetTableName());
    }
    PL_ASSERT(!expr->GetIsBound());
    expr->SetDepth(depth);
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
  expr->GetSubSelect()->Accept(this);
}

void BindNodeVisitor::Visit(expression::StarExpression *expr) {
  if (!BinderContext::HasTables(context_)) {
    throw BinderException("Invalid expression" + expr->GetInfo());
  }
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

  // Check catalog and bind function
  std::vector<type::TypeId> argtypes;
  for (size_t i = 0; i < expr->GetChildrenSize(); i++)
    argtypes.push_back(expr->GetChild(i)->GetValueType());
  // Check and set the function ptr
  // TODO(boweic): Visit the catalog using the interface that is protected by
  // transaction
  const catalog::FunctionData &func_data =
      catalog_->GetFunction(expr->GetFuncName(), argtypes);
  LOG_DEBUG("Function %s found in the catalog", func_data.func_name_.c_str());
  LOG_DEBUG("Argument num: %ld", func_data.argument_types_.size());
  LOG_DEBUG("Is UDF %d", func_data.is_udf_);

  if (!func_data.is_udf_) {
    expr->SetBuiltinFunctionExpressionParameters(
        func_data.func_, func_data.return_type_, func_data.argument_types_);

    // Look into the OperatorId for built-in functions to check the first
    // argument for timestamp functions.
    // TODO(LM): The OperatorId might need to be changed to global ID after we
    // rewrite the function identification logic.
    auto func_operator_id = func_data.func_.op_id;
    if (func_operator_id == OperatorId::DateTrunc ||
        func_operator_id == OperatorId::DatePart) {
      auto date_part = expr->GetChild(0);

      // Test whether the first argument is a correct DatePartType
      StringToDatePartType(
          date_part->Evaluate(nullptr, nullptr, nullptr).ToString());
    }
  } else {
    expr->SetUDFFunctionExpressionParameters(func_data.func_context_,
                                             func_data.return_type_,
                                             func_data.argument_types_);
  }
}

}  // namespace binder
}  // namespace peloton
