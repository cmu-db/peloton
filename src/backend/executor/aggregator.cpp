/*-------------------------------------------------------------------------
 *
 * aggregator.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/executor/aggregator.cpp
 *
 *-------------------------------------------------------------------------
 */


#include "backend/executor/aggregator.h"
#include "backend/common/logger.h"

namespace peloton {
namespace executor {

/*
 * Create an instance of an aggregator for the specified aggregate
 * type, column type, and result type. The object is constructed in
 * memory from the provided memrory pool.
 */
Agg* GetAggInstance(ExpressionType agg_type) {
  Agg* aggregator;

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
    case EXPRESSION_TYPE_AGGREGATE_MAX  :
      aggregator = new MaxAgg();
      break;
    default: {
      std::string message =  "Unknown aggregate type " + std::to_string(agg_type);
      throw UnknownTypeException(agg_type, message);
    }
  }

  return aggregator;
}

/*
 * Helper method responsible for inserting the results of the aggregation
 * into a new tuple in the output tile group as well as passing through any
 * additional columns from the input tile group.
 */
bool Helper(const planner::AggregateNode* node,
            Agg** aggregates,
            storage::DataTable *output_table,
            AbstractTuple *prev_tuple,
            txn_id_t transaction_id) {

  // Ignore null tuples
  if(prev_tuple == nullptr)
    return true;

  auto schema = output_table->GetSchema();
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  /*
   * This first pass is to add all columns that were aggregated on.
   */
  //LOG_TRACE("Setting aggregated columns \n");

  auto aggregate_columns = node->GetAggregateColumns();
  auto aggregate_columns_map = node->GetAggregateColumnsMap();
  for (oid_t column_itr = 0; column_itr < aggregate_columns.size(); column_itr++){
    if (aggregates[column_itr] != nullptr) {
      const oid_t column_index = aggregate_columns_map[column_itr];
      const ValueType column_type = schema->GetType(column_index);
      Value final_val = aggregates[column_itr]->Finalize();
      tuple.get()->SetValue(column_index, final_val.CastAs(column_type));
    }
  }

  /*
   * Execute a second pass to set the output columns from the input
   * columns that are being passed through. These are the columns
   * that are not being aggregated on but are still in the SELECT
   * list.
   */
  //LOG_TRACE("Setting pass through columns \n");

  auto pass_through_columns_map = node->GetPassThroughColumnsMap();
  for (auto column : pass_through_columns_map){
    // <first, second> == <input tuple column index, output tuple column index >
    tuple.get()->SetValue(column.second, prev_tuple->GetValue(column.first));
  }

  std::cout << "GROUP TUPLE :: " << *(tuple.get());

  auto location = output_table->InsertTuple(transaction_id, tuple.get(), false);
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

template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::
Aggregator(const planner::AggregateNode *node,
           storage::DataTable *output_table,
           txn_id_t transaction_id)
           : node(node),
             output_table(output_table),
             transaction_id(transaction_id) {

  group_by_columns = node->GetGroupByColumns();
  group_by_key_schema = node->GetGroupByKeySchema();
  assert(group_by_key_schema != nullptr);

  // Allocate a group by key tuple
  group_by_key_tuple = new storage::Tuple(group_by_key_schema, true);

  aggregate_types = node->GetAggregateTypes();
  aggregate_columns = node->GetAggregateColumns();
}

template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::
~Aggregator(){

  // Clean up group by key tuple
  group_by_key_tuple->FreeUninlinedData();
  delete group_by_key_tuple;

}

template<>
bool
Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::
Advance(AbstractTuple *cur_tuple,
        AbstractTuple *prev_tuple __attribute__((unused))) {
  AggregateList *aggregate_list;

  // Configure a tuple and search for the required group.
  for (oid_t column_itr = 0; column_itr < group_by_columns.size(); column_itr++) {
    Value cur_tuple_val = cur_tuple->GetValue(group_by_columns[column_itr]);
    group_by_key_tuple->SetValue(column_itr, cur_tuple_val);
  }

  auto map_itr = aggregates_map.find(*group_by_key_tuple);

  // Group not found. Make a new entry in the hash for this new group.
  if (map_itr == aggregates_map.end()) {

    // Allocate new aggregate list
    aggregate_list = new AggregateList();
    aggregate_list->aggregates = new Agg*[aggregate_columns.size()];
    aggregate_list->group_tuple = cur_tuple;

    for (oid_t column_itr = 0; column_itr < aggregate_columns.size(); column_itr++) {
      aggregate_list->aggregates[column_itr] =
          GetAggInstance(aggregate_types[column_itr]);
    }

    aggregates_map.
    insert(HashAggregateMapType::value_type(*group_by_key_tuple,
                                            aggregate_list));
  }
  // Otherwise, the list is the second item of the pair.
  else {
    aggregate_list = map_itr->second;
  }

  // Update the aggregation calculation
  for (oid_t column_itr = 0; column_itr < aggregate_columns.size(); column_itr++) {
    const oid_t column_index = aggregate_columns[column_itr];
    Value value = cur_tuple->GetValue(column_index);
    aggregate_list->aggregates[column_itr]->Advance(value);
  }

  return true;
}

template<>
bool
Aggregator<PlanNodeType::PLAN_NODE_TYPE_HASHAGGREGATE>::
Finalize(AbstractTuple *prev_tuple __attribute__((unused))) {

  for (auto entry : aggregates_map){
    if (Helper(node, entry.second->aggregates, output_table,
               entry.second->group_tuple, transaction_id) == false) {
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


//===--------------------------------------------------------------------===//
// Specialization of an aggregator that expects the input table to be
// sorted on the group by key.
//===--------------------------------------------------------------------===//

template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::
Aggregator(const planner::AggregateNode *node,
           storage::DataTable *output_table,
           txn_id_t transaction_id)
           : node(node),
             output_table(output_table),
             transaction_id(transaction_id) {

  group_by_columns = node->GetGroupByColumns();

  // Create aggregators and initialize
  aggregate_types = node->GetAggregateTypes();
  aggregate_columns = node->GetAggregateColumns();
  aggregates = new Agg*[aggregate_columns.size()];
  ::memset(aggregates, 0, sizeof(void*) * aggregate_columns.size());

}

template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::
~Aggregator(){
  // Clean up aggregators
  for (oid_t column_itr = 0; column_itr < aggregate_columns.size(); column_itr++) {
    delete aggregates[column_itr];
  }
  delete[] aggregates;
}

template<>
bool
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::
Advance(AbstractTuple *cur_tuple,
        AbstractTuple *prev_tuple) {

  bool start_new_agg = false;

  // Check if we are starting a new aggregate tuple
  if (prev_tuple == nullptr) {
    LOG_TRACE("Prev tuple is nullprt!");
    start_new_agg = true;
  }
  else {
    // Compare group by columns
    for (oid_t column_itr = 0; column_itr < group_by_columns.size() ; column_itr++) {
      Value lval = cur_tuple->GetValue(group_by_columns[column_itr]);
      Value rval = prev_tuple->GetValue(group_by_columns[column_itr]);
      bool not_equal = lval.OpNotEquals(rval).IsTrue();

      if (not_equal){
        start_new_agg = true;
        break;
      }
    }
  }

  // If we have started a new aggregate tuple
  if (start_new_agg) {
    LOG_TRACE("Started a new group!");

    if (Helper(node, aggregates, output_table,
               prev_tuple, transaction_id) == false){
      return false;
    }

    // Create aggregate
    for (oid_t column_itr = 0; column_itr < aggregate_columns.size(); column_itr++) {
      // Clean up previous aggregate
      delete aggregates[column_itr];
      aggregates[column_itr] =  GetAggInstance(aggregate_types[column_itr]);
    }
  }

  // Update the aggregation calculation
  for (oid_t column_itr = 0; column_itr < aggregate_columns.size(); column_itr++) {
    const oid_t column_index = aggregate_columns[column_itr];
    Value value = cur_tuple->GetValue(column_index);
    aggregates[column_itr]->Advance(value);
  }

  return true;
}

template<>
bool
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::
Finalize(AbstractTuple *prev_tuple) {

  if (Helper(node, aggregates, output_table,
             prev_tuple, transaction_id) == false) {
    return false;
  }

  // TODO: if no record exists in input_table, we have to output a null record
  // only when it doesn't have GROUP BY. See difference of these cases:
  //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
  //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple

  return true;
}

} // namespace executor
} // namespace peloton
