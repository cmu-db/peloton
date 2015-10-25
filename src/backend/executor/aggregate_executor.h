//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// aggregate_executor.h
//
// Identification: src/backend/executor/aggregate_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"

#include <vector>

namespace peloton {
namespace executor {

/**
 * The actual executor class templated on the type of aggregation that
 * should be performed.
 *
 * If it is instantiated using PLAN_NODE_TYPE_AGGREGATE,
 * then it will do a constant space aggregation that expects the input table
 * to be sorted on the group by key.
 *
 * If it is instantiated using PLAN_NODE_TYPE_HASHAGGREGATE,
 * then the input does not need to be sorted and it will hash the group by key
 * to aggregate the tuples.
 */
class AggregateExecutor : public AbstractExecutor {
 public:
  AggregateExecutor(const AggregateExecutor &) = delete;
  AggregateExecutor &operator=(const AggregateExecutor &) = delete;
  AggregateExecutor(AggregateExecutor &&) = delete;
  AggregateExecutor &operator=(AggregateExecutor &&) = delete;

  AggregateExecutor(const planner::AbstractPlan *node,
                    ExecutorContext *executor_context);

  ~AggregateExecutor();

 protected:
  bool DInit();

  bool DExecute();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of aggregate */
  std::vector<LogicalTile *> result;

  /** @brief Result itr */
  oid_t result_itr = INVALID_OID;

  /** @brief Computed the result */
  bool done = false;

  /** @brief Output table. */
  storage::DataTable *output_table = nullptr;
};

}  // namespace executor
}  // namespace peloton
