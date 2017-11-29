//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_executor.cpp
//
// Identification: src/executor/aggregate_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container_tuple.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/aggregate_executor.h"
#include "executor/aggregator.h"
#include "executor/executor_context.h"
#include "executor/logical_tile_factory.h"
#include "planner/aggregate_plan.h"
#include "storage/table_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for aggregate executor.
 * @param node Aggregate node corresponding to this executor.
 */
AggregateExecutor::AggregateExecutor(const planner::AbstractPlan *node,
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
  PL_ASSERT(children_.size() == 1);

  LOG_TRACE("Aggregate executor :: 1 child ");

  // Grab info from plan node and check it
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Construct the output table
  auto output_table_schema =
      const_cast<catalog::Schema *>(node.GetOutputSchema());

  PL_ASSERT(output_table_schema->GetColumnCount() >= 1);

  // clean up result
  result_itr = START_OID;
  result.clear();

  // reset done
  done = false;

  // clean up temporary aggregation table
  delete output_table;

  output_table =
      storage::TableFactory::GetTempTable(output_table_schema, false);

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
        case AggregateType::HASH:
          LOG_TRACE("Use HashAggregator");
          aggregator.reset(new HashAggregator(
              &node, output_table, executor_context_, tile->GetColumnCount()));
          break;
        case AggregateType::SORTED:
          LOG_TRACE("Use SortedAggregator");
          aggregator.reset(new SortedAggregator(
              &node, output_table, executor_context_, tile->GetColumnCount()));
          break;
        case AggregateType::PLAIN:
          LOG_TRACE("Use PlainAggregator");
          aggregator.reset(
              new PlainAggregator(&node, output_table, executor_context_));
          break;
        default:
          LOG_ERROR("Invalid aggregate type. Return.");
          return false;
      }
    }

    LOG_TRACE("Looping over tile..");

    for (oid_t tuple_id : *tile) {
      std::unique_ptr<ContainerTuple<LogicalTile>> cur_tuple(
          new ContainerTuple<LogicalTile>(tile.get(), tuple_id));

      if (aggregator->Advance(cur_tuple.get()) == false) {
        return false;
      }
    }
    LOG_TRACE("Finished processing logical tile");
  }

  LOG_TRACE("Finalizing..");
  if (!aggregator.get() || !aggregator->Finalize()) {
    // If there's no tuples and no group-by, count() aggregations should return
    // 0 according to the test in MySQL.
    // TODO: We only checked whether all AggTerms are counts here. If there're
    // mixed terms, we should return 0 for counts and null for others.
    bool all_count_aggs = true;
    for (oid_t aggno = 0; aggno < node.GetUniqueAggTerms().size(); aggno++) {
      auto agg_type = node.GetUniqueAggTerms()[aggno].aggtype;
      if (agg_type != ExpressionType::AGGREGATE_COUNT &&
          agg_type != ExpressionType::AGGREGATE_COUNT_STAR)
        all_count_aggs = false;
    }

    // If there's no tuples in the table and only if no group-by in the
    // query, we should return a NULL tuple
    // this is required by SQL
    if (!aggregator.get() && node.GetGroupbyColIds().empty()) {
      LOG_TRACE(
          "No tuples received and no group-by. Should insert a NULL tuple "
          "here.");
      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(output_table->GetSchema(), true));
      if (all_count_aggs == true) {
        tuple->SetAllZeros();
      } else {
        tuple->SetAllNulls();
      }
      UNUSED_ATTRIBUTE auto location = output_table->InsertTuple(tuple.get());
      PL_ASSERT(location.block != INVALID_OID);
    } else {
      done = true;
      return false;
    }
  }

  // Transform output table into result
  LOG_TRACE("%s", output_table->GetInfo().c_str());
  auto tile_group_count = output_table->GetTileGroupCount();
  if (tile_group_count == 0 || output_table->GetTupleCount() == 0) return false;

  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = output_table->GetTileGroup(tile_group_itr);
    PL_ASSERT(tile_group != nullptr);
    LOG_TRACE("\n%s", tile_group->GetInfo().c_str());

    // Get the logical tiles corresponding to the given tile group
    auto logical_tile = LogicalTileFactory::WrapTileGroup(tile_group);

    result.push_back(logical_tile);
  }
  LOG_TRACE("%s", result[result_itr]->GetInfo().c_str());

  done = true;

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

}  // namespace executor
}  // namespace peloton
