//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// aggregate_executor.cpp
//
// Identification: src/backend/executor/aggregate_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/executor/aggregator.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/aggregateV2_node.h"
#include "backend/storage/table_factory.h"


#include <utility>
#include <vector>


namespace peloton {
namespace executor {

/**
 * @brief Constructor for aggregate executor.
 * @param node Aggregate node corresponding to this executor.
 */
AggregateExecutor::AggregateExecutor(planner::AbstractPlanNode *node,
                                     ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

AggregateExecutor::~AggregateExecutor() {
  // clean up temporary aggregation table
  delete output_table;
}

/**
 * @brief Basic initialization.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DInit() {
  assert(children_.size() == 1);

  LOG_TRACE("Aggregate Scan executor :: 1 child \n");

  // Grab info from plan node and check it
  const planner::AggregateV2Node &node = GetPlanNode<planner::AggregateV2Node>();

  // Construct the output table
  auto output_table_schema = const_cast<catalog::Schema*>(node.GetOutputSchema());
  size_t column_count = output_table_schema->GetColumnCount();
  assert(column_count >= 1);

  result_itr = START_OID;

  output_table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, output_table_schema, "temp_table");

  return true;
}

/**
 * @brief Creates logical tile(s) wrapping the results of aggregation.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DExecute() {
  // Already performed the aggregation
  if (done) {
    if (result_itr == result.size()) {
      return false;
    } else {
      // Return appropriate tile and go to next tile
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }
  }

  // Grab info from plan node
  const planner::AggregateV2Node &node = GetPlanNode<planner::AggregateV2Node>();

  // Get an aggregator
  Aggregator<PlanNodeType::PLAN_NODE_TYPE_AGGREGATE> aggregator(
      &node, output_table, executor_context_);

  // Get input tiles and aggregate them
  while (children_[0]->Execute() == true) {
    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

    LOG_TRACE("Looping over tile..");
    expression::ContainerTuple<LogicalTile> *prev_tuple = nullptr;

    for (oid_t tuple_id : *tile) {
      auto cur_tuple =
          new expression::ContainerTuple<LogicalTile>(tile.get(), tuple_id);

      if (aggregator.Advance(cur_tuple, prev_tuple) == false) {
        return false;
      }

      delete prev_tuple;
      prev_tuple = cur_tuple;
    }

    LOG_TRACE("Finalizing..");
    if (!aggregator.Finalize(prev_tuple)) return false;

    delete prev_tuple;
    LOG_TRACE("Finished processing logical tile");
  }

  // Transform output table into result
  auto tile_group_count = output_table->GetTileGroupCount();

  if (tile_group_count == 0) return false;

  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = output_table->GetTileGroup(tile_group_itr);

    // Get the logical tiles corresponding to the given tile group
    auto logical_tile = LogicalTileFactory::WrapTileGroup(tile_group);

    result.push_back(logical_tile);
  }

  done = true;
  LOG_TRACE("Result tiles : %lu \n", result.size());

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

}  // namespace executor
}  // namespace peloton
