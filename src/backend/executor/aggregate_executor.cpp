/*-------------------------------------------------------------------------
 *
 * aggregate_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/aggregate_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/common/logger.h"
#include "backend/executor/aggregator.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/planner/aggregate_node.h"
#include "backend/storage/table_factory.h"
#include "backend/expression/container_tuple.h"

#include <utility>
#include <vector>

namespace nstore {
namespace executor {

/**
 * @brief Constructor for aggregate executor.
 * @param node Aggregate node corresponding to this executor.
 */
AggregateExecutor::AggregateExecutor(planner::AbstractPlanNode *node,
                                     Transaction *transaction)
: AbstractExecutor(node, transaction) {
}

/**
 * @brief Basic initialization.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DInit() {
  assert(children_.size() == 1);

  LOG_INFO("Aggregate Scan executor :: 1 child \n");

  // Grab info from plan node and check it
  const planner::AggregateNode &node = GetNode<planner::AggregateNode>();

  // Plan has info about :
  // (1) the aggregate columns, and
  // (2) the pass through columns

  // Construct the output table
  auto output_table_schema = node.GetOutputTableSchema();
  size_t column_count = output_table_schema->GetColumnCount();
  assert(column_count >= 1);

  output_table = storage::TableFactory::GetDataTable(INVALID_OID,
                                                     output_table_schema,
                                                     "temp_table");

  return true;
}

/**
 * @brief Creates logical tile(s) wrapping the results of aggregation.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DExecute() {

  LOG_INFO("Executing.. ");

  // Grab info from plan node
  const planner::AggregateNode &node = GetNode<planner::AggregateNode>();
  txn_id_t txn_id = transaction_->GetTransactionId();
  Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE> aggregator(&node,
                                                                output_table,
                                                                txn_id);

  // Get input tiles and aggregate them
  while(children_[0]->Execute() == true) {
    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

    LOG_INFO("Looping over tile..");
    expression::ContainerTuple<LogicalTile> *prev_tuple = nullptr;

    for (oid_t tuple_id : *tile) {
      auto cur_tuple = new expression::ContainerTuple<LogicalTile>(tile.get(), tuple_id);

      if (aggregator.Advance(cur_tuple, prev_tuple) == false) {
        return false;
      }

      delete prev_tuple;
      prev_tuple = cur_tuple;
    }

    LOG_INFO("Finalizing..");
    if (!aggregator.Finalize(prev_tuple))
      return false;

    LOG_INFO("Finished processing logical tile");
  }

  return true;
}


} // namespace executor
} // namespace nstore

