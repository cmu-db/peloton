//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_set_op_executor.h
//
// Identification: src/include/executor/hash_set_op_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "common/internal_types.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "common/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Hash based set operation executor.
 *
 * @warning This is a pipeline breaker.
 *
 * IMPORTANT: Children must have the same physical schema.
 * TODO: Postgres relaxes this to "compatible schema" (e.g., int -> double).
 *
 * Currently supported: INTERSECT/INTERSECT ALL/EXCEPT/EXCEPT ALL.
 * We use a similar algorithm to Postgres but more optimized.
 * Since the result of all supported set-op must be a subset
 * of the left child,
 * we can simply massage the validation flags of the left child
 * and forward the (logical tiles) upwards.
 * This avoids materialization.
 *
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class HashSetOpExecutor : public AbstractExecutor {
 public:
  HashSetOpExecutor(const HashSetOpExecutor &) = delete;
  HashSetOpExecutor &operator=(const HashSetOpExecutor &) = delete;
  HashSetOpExecutor(const HashSetOpExecutor &&) = delete;
  HashSetOpExecutor &operator=(const HashSetOpExecutor &&) = delete;

  explicit HashSetOpExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

 protected:
  bool DInit();
  bool DExecute();

 private:
  /** @brief Counter-pair type for binary set-op */
  typedef struct {
    size_t left = 0;
    size_t right = 0;
  } counter_pair_t;

  /** @brief Type definitions for hash table */
  typedef std::unordered_map<
      ContainerTuple<LogicalTile>, counter_pair_t,
      ContainerTupleHasher<LogicalTile>,
      ContainerTupleComparator<LogicalTile>> HashSetOpMapType;

  /* Helper functions */

  bool ExecuteHelper();

  template <SetOpType SETOP>
  bool CalculateCopies(HashSetOpMapType &htable);

  /** @brief Hash table */
  HashSetOpMapType htable_;

  /** @brief The specified set-op type */
  SetOpType set_op_ = SetOpType::INVALID;

  /** @brief Hash table is built or not */
  bool hash_done_ = false;

  /** @brief Input tiles from left child */
  std::vector<std::unique_ptr<LogicalTile>> left_tiles_;

  /** @brief Next tile Id in the vector to return */
  size_t next_tile_to_return_ = 0;
};

}  // namespace executor
}  // namespace peloton
