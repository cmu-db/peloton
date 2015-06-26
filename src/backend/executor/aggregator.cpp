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

namespace nstore {
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
            expression::ContainerTuple<LogicalTile> *prev_tuple,
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
  auto aggregate_columns_map = node->GetAggregateColumnsMapping();
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

  auto pass_through_columns_map = node->GetPassThroughColumnsMapping();
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

/**
 * List of aggregates for a specific group.
 */
struct AggregateList {

  // A tuple from the group of tuples being aggregated.
  // Source of pass through columns.
  storage::Tuple *group_tuple;

  // The aggregates for each column for this group
  Agg* aggregates[0];

};

/*
 * Specialization of an aggregator that expects the input table to be
 * sorted on the group by key.
 */
template<>
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::
Aggregator(const planner::AggregateNode *node,
           storage::DataTable *output_table,
           txn_id_t transaction_id)
           : node(node),
             output_table(output_table),
             transaction_id(transaction_id) {

  aggregate_types = node->GetAggregateTypes();
  aggregate_columns = node->GetAggregateColumns();
  group_by_columns = node->GetGroupByColumns();

  LOG_TRACE("Aggregates :: %lu \n", aggregate_columns.size());

  // Create aggregators and initialize
  aggregates = new Agg*[aggregate_columns.size()];
  ::memset(aggregates, 0, sizeof(void*) * aggregate_columns.size());

}

template<>
bool
Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE>::
Advance(expression::ContainerTuple<LogicalTile> *cur_tuple,
        expression::ContainerTuple<LogicalTile> *prev_tuple) {

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
      if (aggregates[column_itr] != nullptr) {
        aggregates[column_itr]->~Agg();
      }
      aggregates[column_itr] =
          GetAggInstance(aggregate_types[column_itr]);
    }
  }

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
Finalize(expression::ContainerTuple<LogicalTile> *prev_tuple) {

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
} // namespace nstore
