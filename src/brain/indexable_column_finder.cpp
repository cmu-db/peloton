//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.cpp
//
// Identification: src/brain/index_selection.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "brain/indexable_column_finder.h"
namespace peloton {
  namespace brain {

    template<typename K>
    inline void visit(K *expr, SqlNodeVisitor *visitor) {
      if (expr != nullptr)  expr->Accept(visitor);
    }

    void IndexableColumnFinder::Visit(const parser::SelectStatement *node) {
      visit(node->from_table, this);
      visit(node->where_clause, this);
      visit(node->order, this);
      visit(node->group_by, this);
    }

    void IndexableColumnFinder::Visit(const parser::JoinDefinition *node) {
      visit(node->left, this);
      visit(node->right, this);
      visit(node->condition, this);
    }

    void IndexableColumnFinder::Visit(const parser::TableRef *node) {
      visit(node->select, this);
      visit(node->join, this);
    }

    void IndexableColumnFinder::Visit(const parser::GroupByDescription *node) {
      if (node->columns != nullptr) {
        for (auto col : *(node->columns)) {
          visit(col, this);
        }
      }
    }

    void IndexableColumnFinder::Visit(const parser::OrderDescription *node) {
      for (auto expr : *(node->exprs))
        visit(expr, this);
    }

    void IndexableColumnFinder::Visit(const parser::InsertStatement *node) {
      visit(node->select, this);
      // TODO add all columns of the table
    }

    void IndexableColumnFinder::Visit(const parser::DeleteStatement *node) {
      visit(node->table_ref, this);
      visit(node->expr, this);

    }

    void IndexableColumnFinder::Visit(const parser::PrepareStatement *node) {
      // TODO figure out
    }

    void IndexableColumnFinder::Visit(const parser::ExecuteStatement *node) {
      // TODO figure out
    }
    void IndexableColumnFinder::Visit(const parser::TransactionStatement *node) {

    }
    void IndexableColumnFinder::Visit(const parser::UpdateStatement *node) {

    }
    void IndexableColumnFinder::Visit(const parser::CopyStatement *node) {

    }
    void IndexableColumnFinder::Visit(const parser::AnalyzeStatement *node) {

    }

    void IndexableColumnFinder::Visit(expression::CaseExpression *expr) {

    }
    void IndexableColumnFinder::Visit(expression::TupleValueExpression *expr) {

    }
  }
}