//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_executor.cpp
//
// Identification: src/executor/hash_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "type/value.h"
#include "executor/logical_tile.h"
#include "executor/hash_executor.h"
#include "planner/hash_plan.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
HashExecutor::HashExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool HashExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);

  // Initialize executor state
  done_ = false;
  result_itr = 0;

  return true;
}

bool HashExecutor::DExecute() {
  LOG_TRACE("Hash Executor");

  if (done_ == false) {
    const planner::HashPlan &node = GetPlanNode<planner::HashPlan>();

    // First, get all the input logical tiles
    while (children_[0]->Execute()) {
      child_tiles_.emplace_back(children_[0]->GetOutput());
    }

    if (child_tiles_.size() == 0) {
      LOG_TRACE("Hash Executor : false -- no child tiles ");
      return false;
    }

    /* *
     * HashKeys is a vector of TupleValue expr
     * from which we construct a vector of column ids that represent the
     * attributes of the underlying table.
     * The hash table is built on top of these hash key attributes
     * */
    auto &hashkeys = node.GetHashKeys();

    // Construct a logical tile
    for (auto &hashkey : hashkeys) {
      PL_ASSERT(hashkey->GetExpressionType() == ExpressionType::VALUE_TUPLE);
      auto tuple_value =
          reinterpret_cast<const expression::TupleValueExpression *>(
              hashkey.get());
      column_ids_.push_back(tuple_value->GetColumnId());
    }

    // Construct the hash table by going over each child logical tile and
    // hashing
    for (size_t child_tile_itr = 0; child_tile_itr < child_tiles_.size();
         child_tile_itr++) {
      auto tile = child_tiles_[child_tile_itr].get();

      // Go over all tuples in the logical tile
      if (tile->GetTupleCount() > 0) {
        output_tile_itrs_.emplace_back(child_tile_itr);
        for (oid_t tuple_id : *tile) {
          // Key : container tuple with a subset of tuple attributes
          // Value : < child_tile offset, tuple offset >
          auto key = HashMapType::key_type(tile, tuple_id, &column_ids_);
          if (hash_table_.find(key) != hash_table_.end()) {
            //If data is already present, remove from output
            //but leave data for hash joins.
            tile->RemoveVisibility(tuple_id);
          }
          hash_table_[key].insert(
              std::make_pair(output_tile_itrs_.size()-1, tuple_id));
        }
      }
    }

    done_ = true;
  }

  if (result_itr < output_tile_itrs_.size()) {
    SetOutput(child_tiles_[output_tile_itrs_[result_itr++]].release());
    LOG_TRACE("Hash Executor : true -- return tile one at a time ");
    return true;
  } else {
    LOG_TRACE("Hash Executor : false -- done ");
    return false;
  }
}

}  // namespace executor
}  // namespace peloton
