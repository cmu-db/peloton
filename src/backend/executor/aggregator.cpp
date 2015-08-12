//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// aggregator.cpp
//
// Identification: src/backend/executor/aggregator.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/aggregator.h"
#include "backend/common/logger.h"

namespace peloton {
namespace executor {

/*
 * Create an instance of an aggregator for the specified aggregate
 * type, column type, and result type. The object is constructed in
 * memory from the provided memrory pool.
 */
Agg *GetAggInstance(ExpressionType agg_type) {
  Agg *aggregator;

  switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
      aggregator = new CountAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
      aggregator = new CountStarAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_SUM:
      aggregator = new SumAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_AVG:
      aggregator = new AvgAgg(false);
      break;
    case EXPRESSION_TYPE_AGGREGATE_WEIGHTED_AVG:
      aggregator = new AvgAgg(true);
      break;
    case EXPRESSION_TYPE_AGGREGATE_MIN:
      aggregator = new MinAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_MAX:
      aggregator = new MaxAgg();
      break;
    default: {
      std::string message = "Unknown aggregate type "
          + std::to_string(agg_type);
      throw UnknownTypeException(agg_type, message);
    }
  }

  return aggregator;
}

/*
 * Helper method responsible for inserting the results of the aggregation
 * into a new tuple in the output tile group as well as passing through any
 * additional columns from the input tile group.
 * FIXME: need to examine Value's uninlined data problem later.
 */
bool Helper(const planner::AggregateV2Node *node, Agg **aggregates,
            storage::DataTable *output_table, AbstractTuple *prev_tuple,
            executor::ExecutorContext* econtext) {
  // Ignore null tuples
  if (prev_tuple == nullptr)
    return true;

  auto schema = output_table->GetSchema();
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  /*
   * 1) Construct a vector of aggregated values
   */
  std::vector<Value> aggregate_values;
  auto& aggregate_terms = node->GetUniqueAggTerms();
  for (oid_t column_itr = 0; column_itr < aggregate_terms.size();
      column_itr++) {
    if (aggregates[column_itr] != nullptr) {
      Value final_val = aggregates[column_itr]->Finalize();
      aggregate_values.push_back(final_val);
    }
  }

  /*
   * 2) Evaluate filter predicate;
   * if fail, just return
   */
  std::unique_ptr<expression::ContainerTuple<std::vector<Value>> > aggref_tuple(
      new expression::ContainerTuple<std::vector<Value>>(&aggregate_values));

  auto predicate = node->GetPredicate();
  if (nullptr != predicate
      && predicate->Evaluate(prev_tuple, aggref_tuple.get(), econtext).IsFalse()) {
    return true;  // Qual fails, do nothing
  }

  /*
   * 3) Construct the tuple to insert using projectInfo
   */
  node->GetProjectInfo()->Evaluate(tuple.get(), prev_tuple, aggref_tuple.get(),
                                   econtext);

  LOG_INFO("Tuple to Output :");
  std::cout << "GROUP TUPLE :: " << *(tuple.get());

  auto location = output_table->InsertTuple(econtext->GetTransaction(),
                                            tuple.get());
  if (location.block == INVALID_OID) {
    LOG_ERROR("Failed to insert tuple \n");
    return false;
  }

  return true;
}

//===--------------------------------------------------------------------===//
// Specialization of an Aggregator that uses a hash map to aggregate
// tuples from the input table, i.e. it does not expect the input
// table to be sorted on the group by key.
//===--------------------------------------------------------------------===//

/*
 template<>
 Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::Aggregator(
 const planner::AggregateV2Node *node, storage::DataTable *output_table,
 executor::ExecutorContext* econtext)
 : node(node),
 output_table(output_table),
 executor_context(econtext) {
 group_by_columns = node->GetGroupByColumns();
 group_by_key_schema = node->GetGroupByKeySchema();
 assert(group_by_key_schema != nullptr);

 // Allocate a group by key tuple
 group_by_key_tuple = new storage::Tuple(group_by_key_schema, true);

 aggregate_types = node->GetAggregateTypes();
 aggregate_columns = node->GetAggregateColumns();
 }

 template<>
 Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::~Aggregator() {
 // Clean up group by key tuple
 group_by_key_tuple->FreeUninlinedData();
 delete group_by_key_tuple;
 }

 template<>
 bool Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::Advance(
 AbstractTuple *cur_tuple,
 AbstractTuple *prev_tuple __attribute__((unused))) {
 AggregateList *aggregate_list;

 // Configure a tuple and search for the required group.
 for (oid_t column_itr = 0; column_itr < group_by_columns.size();
 column_itr++) {
 Value cur_tuple_val = cur_tuple->GetValue(group_by_columns[column_itr]);
 group_by_key_tuple->SetValue(column_itr, cur_tuple_val);
 }

 auto map_itr = aggregates_map.find(*group_by_key_tuple);

 // Group not found. Make a new entry in the hash for this new group.
 if (map_itr == aggregates_map.end()) {
 // Allocate new aggregate list
 aggregate_list = new AggregateList();
 aggregate_list->aggregates = new Agg *[aggregate_columns.size()];
 aggregate_list->group_tuple = cur_tuple;

 for (oid_t column_itr = 0; column_itr < aggregate_columns.size();
 column_itr++) {
 aggregate_list->aggregates[column_itr] = GetAggInstance(
 aggregate_types[column_itr]);
 }

 aggregates_map.insert(
 HashAggregateMapType::value_type(*group_by_key_tuple, aggregate_list));
 }
 // Otherwise, the list is the second item of the pair.
 else {
 aggregate_list = map_itr->second;
 }

 // Update the aggregation calculation
 for (oid_t column_itr = 0; column_itr < aggregate_columns.size();
 column_itr++) {
 const oid_t column_index = aggregate_columns[column_itr];
 Value value = cur_tuple->GetValue(column_index);
 aggregate_list->aggregates[column_itr]->Advance(value);
 }

 return true;
 }

 template<>
 bool Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::Finalize(
 AbstractTuple *prev_tuple __attribute__((unused))) {
 for (auto entry : aggregates_map) {
 if (Helper(node, entry.second->aggregates, output_table,
 entry.second->group_tuple, transaction) == false) {
 return false;
 }

 // Clean up allocated storage
 delete entry.second->aggregates;
 delete entry.second->group_tuple;
 }

 // TODO: if no record exists in input_table, we have to output a null record
 // only when it doesn't have GROUP BY. See difference of these cases:
 //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
 //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple

 return true;
 }

 */

//===--------------------------------------------------------------------===//
// Specialization of an aggregator that expects the input table to be
// sorted on the group by key.
//===--------------------------------------------------------------------===//
template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::Aggregator(
    const planner::AggregateV2Node *node, storage::DataTable *output_table,
    executor::ExecutorContext* econtext)
    : node(node),
      output_table(output_table),
      executor_context(econtext) {

  group_by_columns = node->GetGroupbyColIds();

  // Create aggregators and initialize
  aggregate_terms = node->GetUniqueAggTerms();
  aggregates = new Agg *[aggregate_terms.size()];
  ::memset(aggregates, 0, sizeof(Agg *) * aggregate_terms.size());
}

template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::~Aggregator() {
  // Clean up aggregators
  for (oid_t column_itr = 0; column_itr < aggregate_terms.size();
      column_itr++) {
    delete aggregates[column_itr];
  }
  delete[] aggregates;
}

template<>
bool Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::Advance(
    AbstractTuple *cur_tuple, AbstractTuple *prev_tuple) {
  bool start_new_agg = false;

  // Check if we are starting a new aggregate tuple
  if (prev_tuple == nullptr) {
    LOG_INFO("Prev tuple is nullptr!");
    start_new_agg = true;
  } else {
    // Compare group by columns
    for (oid_t column_itr = 0; column_itr < group_by_columns.size();
        column_itr++) {
      Value lval = cur_tuple->GetValue(group_by_columns[column_itr]);
      Value rval = prev_tuple->GetValue(group_by_columns[column_itr]);
      bool not_equal = lval.OpNotEquals(rval).IsTrue();

      if (not_equal) {
        LOG_INFO("Group-by columns changed.");
        start_new_agg = true;
        break;
      }
    }
  }

  // If we have started a new aggregate tuple
  if (start_new_agg) {
    LOG_INFO("Started a new group!");

    if (Helper(node, aggregates, output_table, prev_tuple,
               this->executor_context) ==
    false) {
      return false;
    }

    // Create aggregate
    for (oid_t column_itr = 0; column_itr < aggregate_terms.size();
        column_itr++) {
      // Clean up previous aggregate
      delete aggregates[column_itr];
      aggregates[column_itr] = GetAggInstance(
          aggregate_terms[column_itr].first);
    }
  }

  // Update the aggregation calculation
  for (oid_t column_itr = 0; column_itr < aggregate_terms.size();
      column_itr++) {
    Value value = aggregate_terms[column_itr].second->Evaluate(cur_tuple,
                                                               nullptr,
                                                               nullptr);
    aggregates[column_itr]->Advance(value);
  }

  return true;
}

template<>
bool Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::Finalize(
    AbstractTuple *prev_tuple) {
  if (Helper(node, aggregates, output_table, prev_tuple,
             this->executor_context) ==
             false) {
    return false;
  }

  // TODO: if no record exists in input_table, we have to output a null record
  // only when it doesn't have GROUP BY. See difference of these cases:
  //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
  //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple

  return true;
}

}  // namespace executor
}  // namespace peloton
