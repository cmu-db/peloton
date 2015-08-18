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
    : AbstractExecutor(node, executor_context) {
}

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

  LOG_INFO("Aggregate Scan executor :: 1 child \n");

  // Grab info from plan node and check it
  const planner::AggregateV2Node &node =
      GetPlanNode<planner::AggregateV2Node>();

  // Construct the output table
  auto output_table_schema =
      const_cast<catalog::Schema*>(node.GetOutputSchema());

  assert(output_table_schema->GetColumnCount() >= 1);

  result_itr = START_OID;

  bool own_schema = false;
  output_table = storage::TableFactory::GetDataTable(INVALID_OID, INVALID_OID,
                                                     output_table_schema,
                                                     "aggregate_temp_table",
                                                     DEFAULT_TUPLES_PER_TILEGROUP,
                                                     own_schema);

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
  const planner::AggregateV2Node &node =
      GetPlanNode<planner::AggregateV2Node>();

  // Get an aggregator
  std::unique_ptr<AbstractAggregator> aggregator(nullptr);

  // Get input tiles and aggregate them
  std::unique_ptr<expression::ContainerTuple<LogicalTile>> prev_tuple(nullptr);
  std::unique_ptr<LogicalTile> prev_tile(nullptr);
  while (children_[0]->Execute() == true) {

    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

    if(nullptr == aggregator.get()){
      // Initialize the aggregator
      switch(node.GetAggregateStrategy()){
        case AGGREGATE_TYPE_HASH:
          LOG_INFO("Use HashAggregator\n");
          aggregator.reset(new HashAggregator(&node, output_table, executor_context_, tile->GetColumnCount()));
          break;
        case AGGREGATE_TYPE_SORT:
          LOG_INFO("Use SortAggregator\n");
          aggregator.reset(new SortAggregator(&node, output_table, executor_context_));
          break;
        default:
          LOG_ERROR("Invalid aggregate type. Return.\n");
          return false;
      }
    }

    LOG_INFO("Looping over tile..");

    for (oid_t tuple_id : *tile) {
      auto cur_tuple = new expression::ContainerTuple<LogicalTile>(tile.get(),
                                                                   tuple_id);

      if (aggregator->Advance(cur_tuple) == false) {
        return false;
      }
      prev_tuple.reset(cur_tuple);
    }

    /*
     * This is critical: We must not release the tile immediately because
     * the prev_tuple may still be referenced in next iteration by some aggregators.
     * To solve this, we always keep two tiles alive --- hopefully not expensive.
     */
    prev_tile.reset(tile.release());
    LOG_INFO("Finished processing logical tile");
  }

  LOG_INFO("Finalizing..");
  if (aggregator == nullptr) {
    // TODO: need to refactor
    LOG_INFO("child returns empty");
    return false;
  }
  if (!aggregator->Finalize())
    return false;

  prev_tile.reset(nullptr);

  // Transform output table into result
  auto tile_group_count = output_table->GetTileGroupCount();

  if (tile_group_count == 0)
    return false;

  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
      tile_group_itr++) {
    auto tile_group = output_table->GetTileGroup(tile_group_itr);

    // Get the logical tiles corresponding to the given tile group
    auto logical_tile = LogicalTileFactory::WrapTileGroup(tile_group);

    result.push_back(logical_tile);
  }

  done = true;
  LOG_INFO("Result tiles : %lu \n", result.size());

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

}  // namespace executor
}  // namespace peloton
