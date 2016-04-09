//
// Created by Rui Wang on 16-4-6.
//

#include <utility>
#include <vector>
#include <backend/common/thread_manager.h>

#include "backend/planner/exchange_hash_plan.h"
#include "backend/executor/exchange_hash_executor.h"
#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/hash_executor.h"
#include "backend/planner/hash_plan.h"
#include "backend/expression/tuple_value_expression.h"
#include "parallel_seq_scan_task_response.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
ExchangeHashExecutor::ExchangeHashExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
  : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool ExchangeHashExecutor::DInit() {
  assert(children_.size() == 1);

  // Initialize executor state
  done_ = false;
  result_itr = 0;

  return true;
}

void ExchangeHashExecutor::BuildHashTableThreadMain(LogicalTile *tile, size_t child_tile_itr) {
  // Construct the hash table by going over given logical tile and
  // hashing
  // Go over all tuples in the logical tile
  for (oid_t tuple_id : *tile) {
    // Key : container tuple with a subset of tuple attributes
    // Value : < child_tile offset, tuple offset >
    bool ok = hash_table_.update_fn(HashMapType::key_type(tile, tuple_id, &column_ids_), [&] (MapValueType& inner) {
      inner.insert(std::make_pair(child_tile_itr, tuple_id));
    });

    if (!ok) {
      hash_table_.upsert(HashMapType::key_type(tile, tuple_id, &column_ids_), [&](MapValueType& inner) {
        // It is possbile this insert would succeed.
        // I won't check since I am using unordered_set, even insert succeed,
        // another won't hurt.
        inner.insert(std::make_pair(child_tile_itr, tuple_id));
      }, MapValueType());

      hash_table_.update_fn(HashMapType::key_type(tile, tuple_id, &column_ids_), [&] (MapValueType& inner) {
        inner.insert(std::make_pair(child_tile_itr, tuple_id));
      });

      // There is no way second update fail.
      // assert(ok == true);
    }
  }

  auto response = new ParallelSeqScanTaskResponse(NoRetValue, nullptr);
  queue_.Put(response);
}

/*
 * exchange_hash_executor has only one child, which should be exchange_seq_scan (assume no index).
 * exchange_hash_executor creates parallel task only when it gets logical tiles for exechagne_seq_scan.
 */
bool ExchangeHashExecutor::DExecute() {
  LOG_INFO("Exchange Hash Executor");

  if (done_ == false) {
    const planner::ExchangeHashPlan& node = GetPlanNode<planner::ExchangeHashPlan>();

    /* *
    * HashKeys is a vector of TupleValue expr
    * from which we construct a vector of column ids that represent the
    * attributes of the underlying table.
    * The hash table is built on top of these hash key attributes
    * */
    auto &hashkeys = node.GetHashKeys();

    for (auto &hashkey : hashkeys) {
      assert(hashkey->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE);
      auto tuple_value =
        reinterpret_cast<const expression::TupleValueExpression *>(
          hashkey.get());
      column_ids_.push_back(tuple_value->GetColumnId());
    }

    // First, get all the input logical tiles
    size_t child_tile_iter = 0;
    while (children_[0]->Execute()) {
      auto tile = children_[0]->GetOutput();
      child_tiles_.emplace_back(tile);
      std::function<void()> f_build_hash_table = std::bind(&ExchangeHashExecutor::BuildHashTableThreadMain, this,
                                                           tile, child_tile_iter);
      child_tile_iter++;
      ThreadManager::GetInstance().AddTask(f_build_hash_table);
    }

    if (child_tiles_.size() == 0) {
      LOG_TRACE("Hash Executor : false -- no child tiles ");
      return false;
    }
    done_ = true;
  }

  // make sure building hashmap is done before return any child tiles.
  size_t task_count = 0;
  while (task_count < child_tiles_.size()) {
    queue_.Get();
    task_count++;
  }

  // Return logical tiles one at a time
  while (result_itr < child_tiles_.size()) {
    if (child_tiles_[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(child_tiles_[result_itr++].release());
      LOG_TRACE("Hash Executor : true -- return tile one at a time ");
      return true;
    }
  }

  LOG_TRACE("Hash Executor : false -- done ");
  return false;

}

}  // namespace executor
}  // namespace peloton
