//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_set_op_executor.h
//
// Identification: src/backend/executor/hash_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Hash executor.
 *
 */
class HashExecutor : public AbstractExecutor {
 public:
  HashExecutor(const HashExecutor &) = delete;
  HashExecutor &operator=(const HashExecutor &) = delete;
  HashExecutor(const HashExecutor &&) = delete;
  HashExecutor &operator=(const HashExecutor &&) = delete;

  explicit HashExecutor(const planner::AbstractPlan *node,
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
      expression::ContainerTuple<LogicalTile>, counter_pair_t,
      expression::ContainerTupleHasher<LogicalTile>,
      expression::ContainerTupleComparator<LogicalTile>> HashMapType;

  /* Helper functions */

  /** @brief Hash table */
  HashMapType htable_;

  /** @brief Input tiles from left child */
  std::vector<std::unique_ptr<LogicalTile>> left_tiles_;
};

} /* namespace executor */
} /* namespace peloton */
