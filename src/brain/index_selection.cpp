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

namespace peloton {
namespace brain {

IndexSelection::IndexSelection(Workload &query_set, IndexSelectionKnobs knobs,
                               concurrency::TransactionContext *txn)
    : query_set_(query_set), context_(knobs), txn_(txn) {}

void IndexSelection::GetBestIndexes(IndexConfiguration &final_indexes) {
  // http://www.vldb.org/conf/1997/P146.PDF
  // Figure 4 of the "Index Selection Tool" paper.
  // Split the workload 'W' into small workloads 'Wi', with each
  // containing one query, and find out the candidate indexes
  // for these 'Wi'
  // Finally, combine all the candidate indexes 'Ci' into a larger
  // set to form a candidate set 'C' for the provided workload 'W'.

  // The best indexes after every iteration
  IndexConfiguration candidate_indexes;
  // Single column indexes that are useful for at least one query
  IndexConfiguration admissible_indexes;

  // Start the index selection.
  for (unsigned long i = 0; i < context_.knobs_.num_iterations_; i++) {
    LOG_DEBUG("******* Iteration %ld **********", i);
    LOG_DEBUG("Candidate Indexes Before: %s",
              candidate_indexes.ToString().c_str());
    GenerateCandidateIndexes(candidate_indexes, admissible_indexes, query_set_);
    LOG_DEBUG("Admissible Indexes: %s", admissible_indexes.ToString().c_str());
    LOG_DEBUG("Candidate Indexes After: %s",
              candidate_indexes.ToString().c_str());

    // Configuration Enumeration
    IndexConfiguration top_candidate_indexes;
    Enumerate(candidate_indexes, top_candidate_indexes, query_set_,
              context_.knobs_.num_indexes_);
    LOG_DEBUG("Top Candidate Indexes: %s",
              candidate_indexes.ToString().c_str());

    candidate_indexes = top_candidate_indexes;

    // Generate multi-column indexes before starting the next iteration.
    // Only do this if there is next iteration.
    if (i < (context_.knobs_.num_iterations_ - 1)) {
      GenerateMultiColumnIndexes(top_candidate_indexes, admissible_indexes,
                                 candidate_indexes);
    }
  }

  final_indexes = candidate_indexes;
}

void IndexSelection::GenerateCandidateIndexes(
    IndexConfiguration &candidate_config, IndexConfiguration &admissible_config,
    Workload &workload) {
  // If there are no admissible indexes, then this is the first iteration.
  // Candidate indexes will be a union of admissible index set of each query.
  if (admissible_config.IsEmpty() && candidate_config.IsEmpty()) {
    for (auto query : workload.GetQueries()) {
      Workload wi(query, workload.GetDatabaseName());

      IndexConfiguration ai;
      GetAdmissibleIndexes(query.first, ai);
      admissible_config.Merge(ai);

      IndexConfiguration pruned_ai;
      PruneUselessIndexes(ai, wi, pruned_ai);
      // Candidate config for the single-column indexes is the union of
      // candidates for each query.
      candidate_config.Merge(pruned_ai);
    }
    LOG_TRACE("Single column candidate indexes: %lu",
              candidate_config.GetIndexCount());
  } else {
    LOG_TRACE("Pruning multi-column indexes");
    IndexConfiguration pruned_ai;
    PruneUselessIndexes(candidate_config, workload, pruned_ai);
    candidate_config.Set(pruned_ai);
  }
}

void IndexSelection::PruneUselessIndexes(IndexConfiguration &config,
                                         Workload &workload,
                                         IndexConfiguration &pruned_config) {
  IndexConfiguration empty_config;
  auto indexes = config.GetIndexes();

  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    bool is_useful = false;

    for (auto query : workload.GetQueries()) {
      IndexConfiguration c;
      c.AddIndexObject(*it);

      Workload w(query, workload.GetDatabaseName());

      auto c1 = ComputeCost(c, w);
      auto c2 = ComputeCost(empty_config, w);
      LOG_TRACE("Cost with index %s is %lf", c.ToString().c_str(), c1);
      LOG_TRACE("Cost without is %lf", c2);

      if (c1 < c2) {
        is_useful = true;
        break;
      }
    }
    // Index is useful if it benefits any query.
    if (is_useful) {
      pruned_config.AddIndexObject(*it);
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
  LOG_TRACE("GREEDY: Starting with the following index: %s",
            indexes.ToString().c_str());
  size_t current_index_count = indexes.GetIndexCount();

  LOG_TRACE("GREEDY: At start: #indexes chosen : %zu, #num_indexes: %zu",
            current_index_count, k);

  if (current_index_count >= k) return;

  double global_min_cost = ComputeCost(indexes, workload);
  double cur_min_cost = global_min_cost;
  double cur_cost;
  std::shared_ptr<HypotheticalIndexObject> best_index;

  // go through till you get top k indexes
  while (current_index_count < k) {
    // this is the set S so far
    auto new_indexes = indexes;
    for (auto const &index : remaining_indexes.GetIndexes()) {
      new_indexes = indexes;
      new_indexes.AddIndexObject(index);
      cur_cost = ComputeCost(new_indexes, workload);
      LOG_TRACE("GREEDY: Considering this index: %s \n with cost: %lf",
                index->ToString().c_str(), cur_cost);
      if (cur_cost < cur_min_cost ||
          (best_index != nullptr && cur_cost == cur_min_cost &&
           new_indexes.ToString() < best_index->ToString())) {
        cur_min_cost = cur_cost;
        best_index = index;
      }
    }

    // if we found a better configuration
    if (cur_min_cost < global_min_cost) {
      LOG_TRACE("GREEDY: Adding the following index: %s",
                best_index->ToString().c_str());
      indexes.AddIndexObject(best_index);
      remaining_indexes.RemoveIndexObject(best_index);
      current_index_count++;
      global_min_cost = cur_min_cost;

      // we are done with all remaining indexes
      if (remaining_indexes.GetIndexCount() == 0) {
        LOG_TRACE("GREEDY: Breaking because nothing more");
        break;
      }
    } else {  // we did not find any better index to add to our current
              // configuration
      LOG_TRACE("GREEDY: Breaking because nothing better found");
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

  auto max_num_indexes = std::min(context_.knobs_.naive_enumeration_threshold_,
                                  context_.knobs_.num_indexes_);

  // Define a set ordering of (index config, cost) and define the ordering in
  // the set
  std::set<std::pair<IndexConfiguration, double>, IndexConfigComparator>
      running_index_config(workload), temp_index_config(workload),
      result_index_config(workload);

  IndexConfiguration new_element;

  // Add an empty configuration as initialization
  IndexConfiguration empty;
  // The running index configuration contains the possible subsets generated so
  // far. It is updated after every iteration
  auto cost_empty = ComputeCost(empty, workload);
  running_index_config.emplace(empty, cost_empty);

  for (auto const &index : indexes.GetIndexes()) {
    // Make a copy of the running index configuration and add each element to it
    temp_index_config = running_index_config;

    for (auto t : temp_index_config) {
      new_element = t.first;
      new_element.AddIndexObject(index);

      // If the size of the subset reaches our threshold, add to result set
      // instead of adding to the running list
      if (new_element.GetIndexCount() >= max_num_indexes) {
        result_index_config.emplace(new_element,
                                    ComputeCost(new_element, workload));
      } else {
        running_index_config.emplace(new_element,
                                     ComputeCost(new_element, workload));
      }
    }
  }

  // Put all the subsets in the result set
  result_index_config.insert(running_index_config.begin(),
                             running_index_config.end());
  // Remove the starting empty set that we added
  result_index_config.erase({empty, cost_empty});

  for (auto index : result_index_config) {
    LOG_TRACE("EXHAUSTIVE: Index: %s, Cost: %lf",
              index.first.ToString().c_str(), index.second);
  }

  // Since the insertion into the sets ensures the order of cost, get the first
  // m configurations
  if (result_index_config.empty()) return;

  // if having no indexes is better (for eg. for insert heavy workload),
  // then don't choose anything
  if (cost_empty < result_index_config.begin()->second) return;

  auto best_m_index = result_index_config.begin()->first;
  top_indexes.Merge(best_m_index);
}

void IndexSelection::GetAdmissibleIndexes(
    std::shared_ptr<parser::SQLStatement> query, IndexConfiguration &indexes) {
  // Find out the indexable columns of the given workload.
  // The following rules define what indexable columns are:
  // 1. A column that appears in the WHERE clause with format
  //    ==> Column OP Expr <==
  //    OP such as {=, <, >, <=, >=, LIKE, etc.}
  //    Column is a table column name.
  // 2. GROUP BY (if present)
  // 3. ORDER BY (if present)
  // 4. all updated columns for UPDATE query.
  switch (query->GetType()) {
    case StatementType::INSERT: {
      auto insert_stmt = dynamic_cast<parser::InsertStatement *>(query.get());
      // If the insert is along with a select statement, i.e another table's
      // select output is fed into this table.
      if (insert_stmt->select != nullptr) {
        IndexColsParseWhereHelper(insert_stmt->select->where_clause.get(),
                                  indexes);
      }
      break;
    }

    case StatementType::DELETE: {
      auto delete_stmt = dynamic_cast<parser::DeleteStatement *>(query.get());
      IndexColsParseWhereHelper(delete_stmt->expr.get(), indexes);
      break;
    }

    case StatementType::UPDATE: {
      auto update_stmt = dynamic_cast<parser::UpdateStatement *>(query.get());
      IndexColsParseWhereHelper(update_stmt->where.get(), indexes);
      break;
    }

    case StatementType::SELECT: {
      auto select_stmt = dynamic_cast<parser::SelectStatement *>(query.get());
      IndexColsParseWhereHelper(select_stmt->where_clause.get(), indexes);
      IndexColsParseOrderByHelper(select_stmt->order, indexes);
      IndexColsParseGroupByHelper(select_stmt->group_by, indexes);
      break;
    }

    default: { LOG_DEBUG("DDL Statement encountered, Ignoring.."); }
  }
}

void IndexSelection::IndexColsParseWhereHelper(
    const expression::AbstractExpression *where_expr,
    IndexConfiguration &config) {
  if (where_expr == nullptr) {
    LOG_DEBUG("No Where Clause Found");
    return;
  }
  auto expr_type = where_expr->GetExpressionType();
  const expression::AbstractExpression *left_child;
  const expression::AbstractExpression *right_child;
  const expression::TupleValueExpression *tuple_child;

  switch (expr_type) {
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOTEQUAL:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_LIKE:
    case ExpressionType::COMPARE_NOTLIKE:
    case ExpressionType::COMPARE_IN:
      // Get left and right child and extract the column name.
      left_child = where_expr->GetChild(0);
      right_child = where_expr->GetChild(1);

      // if where clause is something like a = b, we don't benefit from index
      if (left_child->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
          right_child->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        return;
      }

      // if where clause is something like 1 = 2, we don't benefit from index
      if (left_child->GetExpressionType() == ExpressionType::VALUE_CONSTANT &&
          right_child->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
        return;
      }

      if (left_child->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        PELOTON_ASSERT(right_child->GetExpressionType() !=
                       ExpressionType::VALUE_TUPLE);
        tuple_child =
            dynamic_cast<const expression::TupleValueExpression *>(left_child);
      } else {
        PELOTON_ASSERT(right_child->GetExpressionType() ==
                       ExpressionType::VALUE_TUPLE);
        tuple_child =
            dynamic_cast<const expression::TupleValueExpression *>(right_child);
      }

      if (!tuple_child->GetIsBound()) {
        LOG_ERROR("Query is not bound");
        PELOTON_ASSERT(false);
      }
      IndexObjectPoolInsertHelper(tuple_child->GetBoundOid(), config);

      break;
    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR:
      left_child = where_expr->GetChild(0);
      right_child = where_expr->GetChild(1);
      IndexColsParseWhereHelper(left_child, config);
      IndexColsParseWhereHelper(right_child, config);
      break;
    default:
      LOG_ERROR("Index selection doesn't allow %s in where clause",
                where_expr->GetInfo().c_str());
      PELOTON_ASSERT(false);
  }
}

void IndexSelection::IndexColsParseGroupByHelper(
    std::unique_ptr<parser::GroupByDescription> &group_expr,
    IndexConfiguration &config) {
  if ((group_expr == nullptr) || (group_expr->columns.size() == 0)) {
    LOG_DEBUG("Group by expression not present");
    return;
  }
  auto &columns = group_expr->columns;
  for (auto it = columns.begin(); it != columns.end(); it++) {
    PELOTON_ASSERT((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tuple_value = (expression::TupleValueExpression *)((*it).get());
    IndexObjectPoolInsertHelper(tuple_value->GetBoundOid(), config);
  }
}

void IndexSelection::IndexColsParseOrderByHelper(
    std::unique_ptr<parser::OrderDescription> &order_expr,
    IndexConfiguration &config) {
  if ((order_expr == nullptr) || (order_expr->exprs.size() == 0)) {
    LOG_DEBUG("Order by expression not present");
    return;
  }
  auto &exprs = order_expr->exprs;
  for (auto it = exprs.begin(); it != exprs.end(); it++) {
    PELOTON_ASSERT((*it)->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tuple_value = (expression::TupleValueExpression *)((*it).get());
    IndexObjectPoolInsertHelper(tuple_value->GetBoundOid(), config);
  }
}

void IndexSelection::IndexObjectPoolInsertHelper(
    const std::tuple<oid_t, oid_t, oid_t> &tuple_oid,
    IndexConfiguration &config) {
  auto db_oid = std::get<0>(tuple_oid);
  auto table_oid = std::get<1>(tuple_oid);
  auto col_oid = std::get<2>(tuple_oid);

  // Add the object to the pool.
  HypotheticalIndexObject iobj(db_oid, table_oid, col_oid);
  auto pool_index_obj = context_.pool_.GetIndexObject(iobj);
  if (!pool_index_obj) {
    pool_index_obj = context_.pool_.PutIndexObject(iobj);
  }
  config.AddIndexObject(pool_index_obj);
}

double IndexSelection::ComputeCost(IndexConfiguration &config,
                                   Workload &workload) {
  double cost = 0.0;
  auto queries = workload.GetQueries();
  for (auto query : queries) {
    std::pair<IndexConfiguration, parser::SQLStatement *> state = {
        config, query.first.get()};
    if (context_.memo_.find(state) != context_.memo_.end()) {
      cost += context_.memo_[state];
    } else {
      auto result = WhatIfIndex::GetCostAndBestPlanTree(
          query, config, workload.GetDatabaseName(), txn_);
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

std::shared_ptr<HypotheticalIndexObject> IndexSelection::AddConfigurationToPool(
    HypotheticalIndexObject object) {
  return context_.pool_.PutIndexObject(object);
}

}  // namespace brain
}  // namespace peloton
