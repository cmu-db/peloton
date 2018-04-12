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

#include <algorithm>
#include <set>

#include "brain/index_selection.h"
#include "brain/what_if_index.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

IndexSelection::IndexSelection(Workload &query_set, size_t max_index_cols,
                               size_t enum_threshold, size_t num_indexes)
    : query_set_(query_set),
      context_(max_index_cols, enum_threshold, num_indexes) {}

void IndexSelection::GetBestIndexes(IndexConfiguration &final_indexes) {
  // Figure 4 of the "Index Selection Tool" paper.
  // Split the workload 'W' into small workloads 'Wi', with each
  // containing one query, and find out the candidate indexes
  // for these 'Wi'
  // Finally, combine all the candidate indexes 'Ci' into a larger
  // set to form a candidate set 'C' for the provided workload 'W'.

  IndexConfiguration candidate_indexes;
  IndexConfiguration admissible_indexes;

  // Start the index selection.
  for (unsigned long i = 0; i < context_.num_iterations_; i++) {
    GenerateCandidateIndexes(candidate_indexes, admissible_indexes, query_set_);

    // Configuration Enumeration
    IndexConfiguration top_candidate_indexes;
    Enumerate(candidate_indexes, top_candidate_indexes, query_set_,
              context_.num_indexes_);

    candidate_indexes = top_candidate_indexes;
    GenerateMultiColumnIndexes(top_candidate_indexes, admissible_indexes,
                               candidate_indexes);
  }
  final_indexes = candidate_indexes;
}

void IndexSelection::GenerateCandidateIndexes(
    IndexConfiguration &candidate_config, IndexConfiguration &admissible_config,
    Workload &workload) {
  if (admissible_config.GetIndexCount() == 0) {
    // If there are no admissible indexes, then this is the first iteration.
    // Candidate indexes will be a union of admissible index set of each query.
    for (auto query : workload.GetQueries()) {
      Workload wi(query);

      IndexConfiguration ai;
      GetAdmissibleIndexes(query, ai);
      admissible_config.Merge(ai);

      PruneUselessIndexes(ai, wi);
      candidate_config.Merge(ai);
    }
  } else {
    PruneUselessIndexes(candidate_config, workload);
  }
}

void IndexSelection::PruneUselessIndexes(IndexConfiguration &config,
                                         Workload &workload) {
  IndexConfiguration empty_config;
  auto indexes = config.GetIndexes();
  auto it = indexes.begin();

  while (it != indexes.end()) {
    bool is_useful = false;

    for (auto query : workload.GetQueries()) {
      IndexConfiguration c;
      c.AddIndexObject(*it);

      Workload w(query);

      if (ComputeCost(c, w) > ComputeCost(empty_config, w)) {
        is_useful = true;
        break;
      }
    }
    // Index is useful if it benefits any query.
    if (!is_useful) {
      it = indexes.erase(it);
    } else {
      it++;
    }
  }
}

void IndexSelection::Enumerate(IndexConfiguration &indexes,
                               IndexConfiguration &top_indexes,
                               Workload &workload, size_t num_indexes) {
  // Get the cheapest indexes through exhaustive search upto a threshold
  ExhaustiveEnumeration(indexes, top_indexes, workload);

  // Get all the remaining indexes which can be part of our optimal set
  auto remaining_indexes = indexes - top_indexes;

  // Greedily add the remaining indexes until there is no improvement in the
  // cost or our required size is reached
  GreedySearch(top_indexes, remaining_indexes, workload, num_indexes);
}

void IndexSelection::GreedySearch(IndexConfiguration &indexes,
                                  IndexConfiguration &remaining_indexes,
                                  Workload &workload, size_t k) {
  // Algorithm:
  // 1. Let S = the best m index configuration using the naive enumeration
  // algorithm. If m = k then exit.
  // 2. Pick a new index I such that Cost (S U {I}, W) <= Cost(S U {I'}, W) for
  // any choice of I' != I
  // 3. If Cost (S U {I}) >= Cost(S) then exit
  // Else S = S U {I}
  // 4. If |S| = k then exit

  size_t current_index_count = context_.naive_enumeration_threshold_;

  if (current_index_count >= k) return;

  double global_min_cost = GetCost(indexes, workload);
  double cur_min_cost = global_min_cost;
  double cur_cost;
  std::shared_ptr<IndexObject> best_index;

  // go through till you get top k indexes
  while (current_index_count < k) {
    // this is the set S so far
    auto original_indexes = indexes;
    for (auto index : remaining_indexes.GetIndexes()) {
      indexes = original_indexes;
      indexes.AddIndexObject(index);
      cur_cost = ComputeCost(indexes, workload);
      if (cur_cost < cur_min_cost) {
        cur_min_cost = cur_cost;
        best_index = index;
      }
    }

    // if we found a better configuration
    if (cur_min_cost < global_min_cost) {
      indexes.AddIndexObject(best_index);
      remaining_indexes.RemoveIndexObject(best_index);
      current_index_count++;
      global_min_cost = cur_min_cost;

      // we are done with all remaining indexes
      if (remaining_indexes.GetIndexCount() == 0) {
        break;
      }
    } else {  // we did not find any better index to add to our current
              // configuration
      break;
    }
  }
}

void IndexSelection::ExhaustiveEnumeration(IndexConfiguration &indexes,
                                           IndexConfiguration &top_indexes,
                                           Workload &workload) {
  // Get the best m index configurations using the naive enumeration algorithm
  // The naive algorithm gets all the possible subsets of size <= m and then
  // returns the cheapest m indexes
  assert(context_.naive_enumeration_threshold_ <= indexes.GetIndexCount());

  // Define a set ordering of (index config, cost) and define the ordering in
  // the set
  std::set<std::pair<IndexConfiguration, double>, IndexConfigComparator>
      running_index_config(workload);
  std::set<std::pair<IndexConfiguration, double>, IndexConfigComparator>
      temp_index_config(workload);
  std::set<std::pair<IndexConfiguration, double>, IndexConfigComparator>
      result_index_config(workload);
  IndexConfiguration new_element;

  // Add an empty configuration as initialization
  IndexConfiguration empty;
  // The running index configuration contains the possible subsets generated so
  // far. It is updated after every iteration
  running_index_config.insert({empty, 0.0});

  for (auto index : indexes.GetIndexes()) {
    // Make a copy of the running index configuration and add each element to it
    temp_index_config = running_index_config;

    for (auto t : temp_index_config) {
      new_element = t.first;
      new_element.AddIndexObject(index);

      // If the size of the subset reaches our threshold, add to result set
      // instead of adding to the running list
      if (new_element.GetIndexCount() >=
          context_.naive_enumeration_threshold_) {
        result_index_config.insert(
            {new_element, ComputeCost(new_element, workload)});
      } else {
        running_index_config.insert(
            {new_element, ComputeCost(new_element, workload)});
      }
    }
  }

  // Put all the subsets in the result set
  result_index_config.insert(running_index_config.begin(),
                             running_index_config.end());
  // Remove the starting empty set that we added
  result_index_config.erase({empty, 0.0});

  // Since the insertion into the sets ensures the order of cost, get the first
  // m configurations
  for (auto index_pair : result_index_config) {
    top_indexes.Merge(index_pair.first);
  }
}

void IndexSelection::GetAdmissibleIndexes(parser::SQLStatement *query,
                                          IndexConfiguration &indexes) {
  // Find out the indexable columns of the given workload.
  // The following rules define what indexable columns are:
  // 1. A column that appears in the WHERE clause with format
  //    ==> Column OP Expr <==
  //    OP such as {=, <, >, <=, >=, LIKE, etc.}
  //    Column is a table column name.
  // 2. GROUP BY (if present)
  // 3. ORDER BY (if present)
  // 4. all updated columns for UPDATE query.

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
        IndexColsParseWhereHelper(
            sql_statement.insert_stmt->select->where_clause.get(), indexes);
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
      IndexColsParseWhereHelper(sql_statement.update_stmt->where.get(),
                                indexes);
      break;

    case StatementType::SELECT:
      sql_statement.select_stmt =
          dynamic_cast<parser::SelectStatement *>(query);
      IndexColsParseWhereHelper(sql_statement.select_stmt->where_clause.get(),
                                indexes);
      IndexColsParseOrderByHelper(sql_statement.select_stmt->order, indexes);
      IndexColsParseGroupByHelper(sql_statement.select_stmt->group_by, indexes);
      break;

    default:
      LOG_WARN("Cannot handle DDL statements");
      PELOTON_ASSERT(false);
  }
}

void IndexSelection::IndexColsParseWhereHelper(
    const expression::AbstractExpression *where_expr,
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
        tuple_child =
            dynamic_cast<const expression::TupleValueExpression *>(left_child);
      } else {
        assert(right_child->GetExpressionType() == ExpressionType::VALUE_TUPLE);
        tuple_child =
            dynamic_cast<const expression::TupleValueExpression *>(right_child);
      }

      if (!tuple_child->GetIsBound()) {
        LOG_INFO("Query is not bound");
        assert(false);
      }
      IndexObjectPoolInsertHelper(tuple_child->GetBoundOid(), config);

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
      LOG_ERROR("Index selection doesn't allow %s in where clause",
                where_expr->GetInfo().c_str());
      assert(false);
  }
  (void)config;
}

void IndexSelection::IndexColsParseGroupByHelper(
    std::unique_ptr<parser::GroupByDescription> &group_expr,
    IndexConfiguration &config) {
  if ((group_expr == nullptr) || (group_expr->columns.size() == 0)) {
    LOG_INFO("Group by expression not present");
    return;
  }
  auto &columns = group_expr->columns;
  for (auto it = columns.begin(); it != columns.end(); it++) {
    assert((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tuple_value = (expression::TupleValueExpression *)((*it).get());
    IndexObjectPoolInsertHelper(tuple_value->GetBoundOid(), config);
  }
}

void IndexSelection::IndexColsParseOrderByHelper(
    std::unique_ptr<parser::OrderDescription> &order_expr,
    IndexConfiguration &config) {
  if ((order_expr == nullptr) || (order_expr->exprs.size() == 0)) {
    LOG_INFO("Order by expression not present");
    return;
  }
  auto &exprs = order_expr->exprs;
  for (auto it = exprs.begin(); it != exprs.end(); it++) {
    assert((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tuple_value = (expression::TupleValueExpression *)((*it).get());
    IndexObjectPoolInsertHelper(tuple_value->GetBoundOid(), config);
  }
}

void IndexSelection::IndexObjectPoolInsertHelper(
    const std::tuple<oid_t, oid_t, oid_t> tuple_oid,
    IndexConfiguration &config) {
  auto db_oid = std::get<0>(tuple_oid);
  auto table_oid = std::get<1>(tuple_oid);
  auto col_oid = std::get<2>(tuple_oid);

  // Add the object to the pool.
  IndexObject iobj(db_oid, table_oid, col_oid);
  auto pool_index_obj = context_.pool_.GetIndexObject(iobj);
  if (!pool_index_obj) {
    pool_index_obj = context_.pool_.PutIndexObject(iobj);
  }
  config.AddIndexObject(pool_index_obj);
}

double IndexSelection::GetCost(IndexConfiguration &config,
                               Workload &workload) const {
  double cost = 0.0;
  auto queries = workload.GetQueries();
  for (auto query : queries) {
    std::pair<IndexConfiguration, parser::SQLStatement *> state = {config,
                                                                   query};
    PELOTON_ASSERT(context_.memo_.find(state) != context_.memo_.end());
    cost += context_.memo_.find(state)->second;
  }
  return cost;
}

double IndexSelection::ComputeCost(IndexConfiguration &config,
                                   Workload &workload) {
  double cost = 0.0;
  auto queries = workload.GetQueries();
  for (auto query : queries) {
    std::pair<IndexConfiguration, parser::SQLStatement *> state = {config,
                                                                   query};
    if (context_.memo_.find(state) != context_.memo_.end()) {
      cost += context_.memo_[state];
    } else {
      auto result =
          WhatIfIndex::GetCostAndPlanTree(query, config, DEFAULT_DB_NAME);
      context_.memo_[state] = result->cost;
      cost += result->cost;
    }
  }
  return cost;
}

void IndexSelection::CrossProduct(
    const IndexConfiguration &config,
    const IndexConfiguration &single_column_indexes,
    IndexConfiguration &result) {
  auto indexes = config.GetIndexes();
  auto columns = single_column_indexes.GetIndexes();
  for (auto index : indexes) {
    for (auto column : columns) {
      if (!index->IsCompatible(column)) continue;
      auto merged_index = (index->Merge(column));
      result.AddIndexObject(context_.pool_.PutIndexObject(merged_index));
    }
  }
}

void IndexSelection::GenerateMultiColumnIndexes(
    IndexConfiguration &config, IndexConfiguration &single_column_indexes,
    IndexConfiguration &result) {
  CrossProduct(config, single_column_indexes, result);
}

std::shared_ptr<IndexObject> IndexSelection::AddConfigurationToPool(
    IndexObject object) {
  return context_.pool_.PutIndexObject(object);
}

}  // namespace brain
}  // namespace peloton
