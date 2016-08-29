//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_executor.h
//
// Identification: src/include/executor/hash_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <unordered_map>
#include <unordered_set>

#include "common/types.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "expression/container_tuple.h"

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

  /** @brief Type definitions for hash table */
  typedef std::unordered_map<
      expression::ContainerTuple<LogicalTile>,
      std::unordered_set<std::pair<size_t, oid_t>,
                         boost::hash<std::pair<size_t, oid_t>>>,
      expression::ContainerTupleHasher<LogicalTile>,
      expression::ContainerTupleComparator<LogicalTile>> HashMapType;

  inline HashMapType &GetHashTable() { return this->hash_table_; }

  inline const std::vector<oid_t> &GetHashKeyIds() const {
    return this->column_ids_;
  }

 protected:
  bool DInit();

  bool DExecute();

 private:
  /** @brief Hash table */
  HashMapType hash_table_;

  /** @brief Input tiles from child node */
  std::vector<std::unique_ptr<LogicalTile>> child_tiles_;

  std::vector<oid_t> column_ids_;

  bool done_ = false;

  size_t result_itr = 0;
};

} /* namespace executor */
} /* namespace peloton */
