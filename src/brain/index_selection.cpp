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
#include "brain/what_if_index.h"
#include <include/parser/statements.h>
#include <include/parser/statements.h>
#include "common/logger.h"
#include <algorithm>
#include <set>

namespace peloton {
namespace brain {

IndexSelection::IndexSelection(Workload &query_set) :
  query_set_(query_set) {
}

std::unique_ptr<IndexConfiguration> IndexSelection::GetBestIndexes() {
  std::unique_ptr<IndexConfiguration> C(new IndexConfiguration());
  // Figure 4 of the "Index Selection Tool" paper.
  // Split the workload 'W' into small workloads 'Wi', with each
  // containing one query, and find out the candidate indexes
  // for these 'Wi'
  // Finally, combine all the candidate indexes 'Ci' into a larger
  // set to form a candidate set 'C' for the provided workload 'W'.
  auto queries = query_set_.GetQueries();
  for (auto query : queries) {
    // Get admissible indexes 'Ai'
    IndexConfiguration Ai;
    GetAdmissibleIndexes(query, Ai);

    Workload Wi;
    Wi.AddQuery(query);

    // Get candidate indexes 'Ci' for the workload.
    IndexConfiguration Ci;
    Enumerate(Ai, Ci, Wi);

    // Add the 'Ci' to the union Index Configuration set 'C'
    C->Add(Ci);
  }
  return C;
}


// Enumerate()
// Given a set of indexes, this function
// finds out the set of cheapest indexes for the workload.
void IndexSelection::Enumerate(IndexConfiguration &indexes,
                               IndexConfiguration &chosen_indexes,
                               Workload &workload) {

  auto top_indexes = ExhaustiveEnumeration(indexes, workload);

  auto remaining_indexes = GetRemainingIndexes(indexes, top_indexes);
  (void)chosen_indexes;

}


void IndexSelection::GreedySearch(IndexConfiguration &indexes,
                                           IndexConfiguration &chosen_indexes,
                                           Workload &workload) {


  (void)indexes;
  (void)chosen_indexes;
  (void)workload;

}

IndexConfiguration IndexSelection::GetRemainingIndexes(IndexConfiguration &indexes, IndexConfiguration top_indexes) {
  return (indexes - top_indexes);
}


IndexConfiguration IndexSelection::ExhaustiveEnumeration(IndexConfiguration &indexes,
                               Workload &workload) {
  unsigned long m = 2;

  assert(m <= indexes.GetIndexCount());

  std::set<IndexConfiguration, Comp> running_set(workload);
  std::set<IndexConfiguration, Comp> temp_set(workload);
  std::set<IndexConfiguration, Comp> result_set(workload);
  IndexConfiguration new_element;
  IndexConfiguration top_indexes;

  IndexConfiguration empty;
  running_set.insert(empty);


  for (auto i : indexes.GetIndexes()) {
    temp_set = running_set;

    for(auto t : temp_set) {
      new_element = t;
      new_element.AddIndexObject(i);

      if(new_element.GetIndexCount() >= m) {
        result_set.insert(new_element);
      } else {
        running_set.insert(new_element);
      }
    }

  }


  result_set.insert(running_set.begin(), running_set.end());
  result_set.erase(empty);


  // combine all the index configurations and return
  for (auto i : result_set) {
    top_indexes.Add(i);
  }

  return top_indexes;
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
  if (where_expr == nullptr) {
    LOG_INFO("No Where Clause Found");
    return;
  }
  auto expr_type = where_expr->GetExpressionType();
  const expression::AbstractExpression *left_child;
  const expression::AbstractExpression *right_child;
  const expression::TupleValueExpression *tuple_child;

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
        assert(right_child->GetExpressionType() != ExpressionType::VALUE_TUPLE);
        tuple_child = dynamic_cast<const expression::TupleValueExpression*> (left_child);
      } else {
        assert(right_child->GetExpressionType() == ExpressionType::VALUE_TUPLE);
        tuple_child = dynamic_cast<const expression::TupleValueExpression*> (right_child);
      }

      if (!tuple_child->GetIsBound()) {
        LOG_INFO("Query is not bound");
        assert(false);
      }
      IndexObjectPoolInsertHelper(tuple_child, config);

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
  if ((group_expr == nullptr) || (group_expr->columns.size() == 0)) {
    LOG_INFO("Group by expression not present");
    return;
  }
  auto &columns = group_expr->columns;
  for (auto it = columns.begin(); it != columns.end(); it++) {
    assert((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tuple_value = (expression::TupleValueExpression*) ((*it).get());
    IndexObjectPoolInsertHelper(tuple_value, config);
  }
}

void IndexSelection::IndexColsParseOrderByHelper(std::unique_ptr<OrderDescription> &order_expr,
                                                 IndexConfiguration &config) {
  if ((order_expr == nullptr) || (order_expr->exprs.size() == 0)) {
    LOG_INFO("Order by expression not present");
    return;
  }
  auto &exprs = order_expr->exprs;
  for (auto it = exprs.begin(); it != exprs.end(); it++) {
    assert((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tuple_value = (expression::TupleValueExpression*) ((*it).get());
    IndexObjectPoolInsertHelper(tuple_value, config);
  }
}

void IndexSelection::IndexObjectPoolInsertHelper(const expression::TupleValueExpression *tuple_col,
                                                 IndexConfiguration &config) {
  auto db_oid = std::get<0>(tuple_col->GetBoundOid());
  auto table_oid = std::get<1>(tuple_col->GetBoundOid());
  auto col_oid = std::get<2>(tuple_col->GetBoundOid());

  // Add the object to the pool.
  IndexObject iobj(db_oid, table_oid, col_oid);
  auto pool_index_obj = context_.pool.GetIndexObject(iobj);
  if (!pool_index_obj) {
    pool_index_obj = context_.pool.PutIndexObject(iobj);
  }
  config.AddIndexObject(pool_index_obj);
}

double IndexSelection::GetCost(IndexConfiguration &config, Workload &workload) const {
  double cost = 0.0;
  auto queries = workload.GetQueries();
  for (auto query : queries) {
    std::pair<IndexConfiguration, parser::SQLStatement *> state = {config, query};
    PL_ASSERT(context_.memo_.find(state) != context_.memo_.end());
    cost += context_.memo_.find(state)->second;
  }
  return cost;
}

double IndexSelection::ComputeCost(IndexConfiguration &config, Workload &workload) {
  double cost = 0.0;
  auto queries = workload.GetQueries();
  for (auto query : queries) {
    std::pair<IndexConfiguration, parser::SQLStatement *> state = {config, query};
    if (context_.memo_.find(state) != context_.memo_.end()) {
      cost += context_.memo_[state];
    } else {
      auto result = WhatIfIndex::GetCostAndPlanTree(query, config, DEFAULT_DB_NAME);
      context_.memo_[state] = result->cost;
      cost += result->cost;
    }
  }
  return cost;
}

}  // namespace brain
}  // namespace peloton
