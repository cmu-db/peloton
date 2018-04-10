//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.cpp
//
// Identification: src/brain/index_selection.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include <include/parser/statements.h>
#include "common/logger.h"

namespace peloton {
namespace brain {

IndexSelection::IndexSelection(std::shared_ptr<Workload> query_set) {
  query_set_ = query_set;
}

std::unique_ptr<IndexConfiguration> IndexSelection::GetBestIndexes() {
  std::unique_ptr<IndexConfiguration> C(new IndexConfiguration());
  // Figure 4 of the "Index Selection Tool" paper.
  // Split the workload 'W' into small workloads 'Wi', with each
  // containing one query, and find out the candidate indexes
  // for these 'Wi'
  // Finally, combine all the candidate indexes 'Ci' into a larger
  // set to form a candidate set 'C' for the provided workload 'W'.
  auto queries = query_set_->GetQueries();
  for (auto query : queries) {
    // Get admissible indexes 'Ai'
    IndexConfiguration Ai;
    GetAdmissibleIndexes(query, Ai);

    Workload Wi;
    Wi.AddQuery(query);

    // Get candidate indexes 'Ci' for the workload.
    IndexConfiguration Ci;
    Enumerate(Ai, Ci, Wi);

    // Add the 'Ci' to the union Indexconfiguration set 'C'
    C->Add(Ci);
  }
  return C;
}

// TODO: [Siva]
// Enumerate()
// Given a set of indexes, this function
// finds out the set of cheapest indexes for the workload.
void IndexSelection::Enumerate(IndexConfiguration &indexes,
                               IndexConfiguration &chosen_indexes,
                               Workload &workload) {
  (void)indexes;
  (void)chosen_indexes;
  (void)workload;
  return;
}

// GetAdmissibleIndexes()
// Find out the indexable columns of the given workload.
// The following rules define what indexable columns are:
// 1. A column that appears in the WHERE clause with format
//    ==> Column OP Expr <==
//    OP such as {=, <, >, <=, >=, LIKE, etc.}
//    Column is a table column name.
// 2. GROUP BY (if present)
// 3. ORDER BY (if present)
// 4. all updated columns for UPDATE query.
void IndexSelection::GetAdmissibleIndexes(parser::SQLStatement *query,
                                          IndexConfiguration &indexes) {
  union {
    parser::SelectStatement *select_stmt;
    parser::UpdateStatement *update_stmt;
    parser::DeleteStatement *delete_stmt;
    parser::InsertStatement *insert_stmt;
  } sql_statement;

  switch (query->GetType()) {
    case StatementType::INSERT:
      sql_statement.insert_stmt =
          dynamic_cast<parser::InsertStatement *>(query);
      // If the insert is along with a select statement, i.e another table's
      // select output is fed into this table.
      if (sql_statement.insert_stmt->select != nullptr) {
        IndexColsParseWhereHelper(sql_statement.insert_stmt->select->where_clause.get(), indexes);
      }
      break;

    case StatementType::DELETE:
      sql_statement.delete_stmt =
        dynamic_cast<parser::DeleteStatement *>(query);
      IndexColsParseWhereHelper(sql_statement.delete_stmt->expr.get(), indexes);
      break;

    case StatementType::UPDATE:
      sql_statement.update_stmt =
        dynamic_cast<parser::UpdateStatement *>(query);
      IndexColsParseWhereHelper(sql_statement.update_stmt->where.get(), indexes);
      break;

    case StatementType::SELECT:
      sql_statement.select_stmt =
        dynamic_cast<parser::SelectStatement *>(query);
      IndexColsParseWhereHelper(sql_statement.select_stmt->where_clause.get(), indexes);
      IndexColsParseOrderByHelper(sql_statement.select_stmt->order, indexes);
      IndexColsParseGroupByHelper(sql_statement.select_stmt->group_by, indexes);
      break;

    default:
      LOG_WARN("Cannot handle DDL statements");
      PL_ASSERT(false);
  }
}

void IndexSelection::IndexColsParseWhereHelper(const expression::AbstractExpression *where_expr,
                                               IndexConfiguration &config) {
  auto expr_type = where_expr->GetExpressionType();
  const expression::AbstractExpression *left_child;
  const expression::AbstractExpression *right_child;
  expression::TupleValueExpression *tuple_child;

  switch (expr_type) {
    case ExpressionType::COMPARE_EQUAL:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_NOTEQUAL:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_GREATERTHAN:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_LESSTHAN:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_LIKE:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_NOTLIKE:
      PELOTON_FALLTHROUGH;
    case ExpressionType::COMPARE_IN:
      // Get left and right child and extract the column name.
      left_child = where_expr->GetChild(0);
      right_child = where_expr->GetChild(1);

      if (left_child->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        tuple_child = (expression::TupleValueExpression *)(left_child);
      } else {
        assert(right_child->GetExpressionType() == ExpressionType::VALUE_TUPLE);
        tuple_child = (expression::TupleValueExpression *)(right_child);
      }
      (void) tuple_child;

      break;
    case ExpressionType::CONJUNCTION_AND:
      PELOTON_FALLTHROUGH;
    case ExpressionType::CONJUNCTION_OR:
      left_child = where_expr->GetChild(0);
      right_child = where_expr->GetChild(1);
      IndexColsParseWhereHelper(left_child, config);
      IndexColsParseWhereHelper(right_child, config);
      break;
    default:
      LOG_ERROR("Index selection doesn't allow %s in where clause", where_expr->GetInfo().c_str());
      assert(false);
  }
  (void)config;
}

void IndexSelection::IndexColsParseGroupByHelper(std::unique_ptr<GroupByDescription> &group_expr,
                                                 IndexConfiguration &config) {
  auto &columns = group_expr->columns;
  for (auto it = columns.begin(); it != columns.end(); it++) {
    assert((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    //auto tuple_value = (expression::TupleValueExpression*) ((*it).get());
    //(void) tuple_value;
    // TODO
    // config.AddIndexObj(tuple_value->GetColumnName());
  }
  (void) config;
}

void IndexSelection::IndexColsParseOrderByHelper(std::unique_ptr<OrderDescription> &order_expr,
                                                 IndexConfiguration &config) {
  auto &exprs = order_expr->exprs;
  for (auto it = exprs.begin(); it != exprs.end(); it++) {
    assert((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    //auto tuple_value = (expression::TupleValueExpression*) ((*it).get());
    //(void) tuple_value;
  }
  (void) config;
}

}  // namespace brain
}  // namespace peloton
