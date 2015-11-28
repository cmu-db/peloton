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
#include <unordered_set>

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/expression/container_tuple.h"

#include <boost/functional/hash.hpp>

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
  /** @brief Type definitions for hash table */
  typedef std::unordered_map<expression::ContainerTuple<LogicalTile>,
      std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>>,
      expression::ContainerTupleHasher<LogicalTile>,
      expression::ContainerTupleComparator<LogicalTile> > HashMapType;

  /** @brief Hash table */
  HashMapType htable_;

  /** @brief Input tiles from child node */
  std::vector<std::unique_ptr<LogicalTile> > child_tiles_;

};

} /* namespace executor */
} /* namespace peloton */
