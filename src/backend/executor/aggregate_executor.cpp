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

#include <utility>
#include <vector>

#include "backend/common/logger.h"
#include "backend/executor/aggregator.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for aggregate executor.
 * @param node Aggregate node corresponding to this executor.
 */
AggregateExecutor::AggregateExecutor(planner::AbstractPlan *node,
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

  LOG_TRACE("Aggregate executor :: 1 child \n");

  // Grab info from plan node and check it
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Construct the output table
  auto output_table_schema =
      const_cast<catalog::Schema*>(node.GetOutputSchema());

  assert(output_table_schema->GetColumnCount() >= 1);

  // clean up result
  result_itr = START_OID;
  result.clear();

  // reset done
  done = false;

  // clean up temporary aggregation table
  delete output_table;

  bool own_schema = false;
  bool adapt_table = false;
  output_table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, output_table_schema, "aggregate_temp_table",
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema, adapt_table);

  return true;
}

/**
 * @brief Creates logical tile(s) wrapping the results of aggregation.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DExecute() {
  // Already performed the aggregation
  if (done) {
    if (result_itr == INVALID_OID || result_itr == result.size()) {
      return false;
    } else {
      // Return appropriate tile and go to next tile
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }
  }

  // Grab info from plan node
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Get an aggregator
  std::unique_ptr<AbstractAggregator> aggregator(nullptr);

  // Get input tiles and aggregate them
  while (children_[0]->Execute() == true) {

    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

    if (nullptr == aggregator.get()) {
      // Initialize the aggregator
      switch (node.GetAggregateStrategy()) {
        case AGGREGATE_TYPE_HASH:
          LOG_INFO("Use HashAggregator\n")
          ;
          aggregator.reset(
              new HashAggregator(&node, output_table, executor_context_,
                                 tile->GetColumnCount()));
          break;
        case AGGREGATE_TYPE_SORTED:
          LOG_INFO("Use SortedAggregator\n")
          ;
          aggregator.reset(
              new SortedAggregator(&node, output_table, executor_context_,
                                   tile->GetColumnCount()));
          break;
        case AGGREGATE_TYPE_PLAIN:
          LOG_INFO("Use PlainAggregator\n")
          ;
          aggregator.reset(
              new PlainAggregator(&node, output_table, executor_context_));
          break;
        default:
          LOG_ERROR("Invalid aggregate type. Return.\n")
          ;
          return false;
      }
    }

    LOG_INFO("Looping over tile..");

    for (oid_t tuple_id : *tile) {

      std::unique_ptr<expression::ContainerTuple<LogicalTile>> cur_tuple(
          new expression::ContainerTuple<LogicalTile>(tile.get(), tuple_id));

      if (aggregator->Advance(cur_tuple.get()) == false) {
        return false;
      }

    }
    LOG_TRACE("Finished processing logical tile");
  }

  LOG_INFO("Finalizing..");
  if (!aggregator.get() || !aggregator->Finalize()) {

    // If there's no tuples in the table and only if no group-by in the query,
    // we should return a NULL tuple
    // this is required by SQL
    if(!aggregator.get() && node.GetGroupbyColIds().empty()){
      LOG_INFO("No tuples received and no group-by. Should insert a NULL tuple here.");
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(output_table->GetSchema(), true));
      tuple->SetAllNulls();
      output_table->InsertTuple(executor_context_->GetTransaction(), tuple.get());
    }
    else{
      done = true;
      return false;
    }
  }

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
