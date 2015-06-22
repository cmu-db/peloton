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

#include <utility>
#include <vector>

namespace nstore {
namespace executor {

/**
 * @brief Constructor for aggregate executor.
 * @param node Aggregate node corresponding to this executor.
 */
template<PlanNodeType aggregate_type>
AggregateExecutor<aggregate_type>::AggregateExecutor(planner::AbstractPlanNode *node)
: AbstractExecutor(node) {
}

/**
 * @brief Basic initialization.
 * @return true on success, false otherwise.
 */
template<PlanNodeType aggregate_type>
bool AggregateExecutor<aggregate_type>::DInit() {
  assert(children_.size() == 1);

  LOG_TRACE("Aggregate Scan executor :: 1 child \n");

  // Grab info from plan node and check it
  const planner::AggregateNode &node = GetNode<planner::AggregateNode>();

  // Plan has info about :
  //
  // (1) the aggregate columns, and
  // (2) the pass through columns

  // Construct the output table
  catalog::Schema *output_table_schema = node.GetOutputTableSchema();
  size_t column_count = output_table_schema->GetColumnCount();
  assert(column_count >= 1);

  output_table = storage::TableFactory::GetDataTable(INVALID_OID,
                                                     output_table_schema,
                                                     "temp_table",
                                                     DEFAULT_TUPLES_PER_TILEGROUP);

  return true;
}

/**
 * @brief Creates logical tile(s) wrapping the results of aggregation.
 * @return true on success, false otherwise.
 */
template<PlanNodeType aggregate_type>
bool AggregateExecutor<aggregate_type>::DExecute() {

  /*
  // Grab info from plan node
  const planner::AggregateNode &node = GetNode<planner::AggregateNode>();

  auto agg_types = node.GetAggregateTypes();
  auto group_by_key_schema = node.GetGroupBySchema();

  // Get input tile and aggregate it
  std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());
  */

  /*
  for (oid_t tuple_id : *tile) {
    expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);

  }

  std::vector<int> groupByColumns = node->getGroupByColumns();
  storage::Tuple *prev(input_table->schema());

  Aggregator<aggregate_type> aggregator(group_by_key_schema,
                                       node, &m_passThroughColumns,
                                       input_table, output_table,
                                       &agg_types,
                                       &groupByColumns, &col_types);

  LOG_TRACE("looping..");
  for (TableTuple cur(input_table->schema()); it.next(cur);
      prev.move(cur.address()))
  {
    if (!aggregator.nextTuple( cur, prev))
    {
      return false;
    }
  }
  */

  LOG_TRACE("finalizing..");

  //if (!aggregator.Finalize(prev))
  //  return false;

  LOG_TRACE("finished");

  return true;
}


} // namespace executor
} // namespace nstore

