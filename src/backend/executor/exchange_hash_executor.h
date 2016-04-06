//
// Created by Rui Wang on 16-4-6.
//

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "backend/common/types.h"
#include "libcuckoo/cuckoohash_map.hh"
#include "backend/executor/abstract_exchange_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/expression/container_tuple.h"
#include "abstract_scan_executor.h"

#include <boost/functional/hash.hpp>

namespace peloton {

namespace executor {

class ExchangeHashExecutor : public AbstractExchangeExecutor, public AbstractExecutor {
public:
  ExchangeHashExecutor(const ExchangeHashExecutor &) = delete;
  ExchangeHashExecutor &operator=(const ExchangeHashExecutor &) = delete;
  ExchangeHashExecutor(ExchangeHashExecutor &&) = delete;
  ExchangeHashExecutor &operator=(ExchangeHashExecutor &&) = delete;

  explicit ExchangeHashExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context);

// typedef std::unordered_map<
//    expression::ContainerTuple<LogicalTile>,
//    std::unordered_set<std::pair<size_t, oid_t>,
//    boost::hash<std::pair<size_t, oid_t>>>,
//    expression::ContainerTupleHasher<LogicalTile>,
//    expression::ContainerTupleComparator<LogicalTile>> HashMapType;

  typedef std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>> MapValueType;

  typedef cuckoohash_map<
    expression::ContainerTuple<LogicalTile>,
    std::unordered_set<std::pair<size_t, oid_t>,
    boost::hash<std::pair<size_t, oid_t>>>,
    expression::ContainerTupleHasher<LogicalTile>,
    expression::ContainerTupleComparator<LogicalTile>
    > HashMapType;

  inline HashMapType &GetHashTable() { return this->hash_table_; }

  inline const std::vector<oid_t> &GetHashKeyIds() const {
    return this->column_ids_;
  }

  void BuildHashTableThreadMain(HashMapType *table, LogicalTile * tile, size_t child_tile_itr,
                              BlockingQueue<AbstractParallelTaskResponse *> *queue);

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


}  // namespace executor
}   // namespace peloton

