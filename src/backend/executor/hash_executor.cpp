//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_set_op_executor.cpp
//
// Identification: src/backend/executor/hash_set_op_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/hash_executor.h"
#include "backend/planner/hash_plan.h"
#include "backend/expression/tuple_value_expression.h"

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
  assert(children_.size() == 1);
  return true;
}

bool HashExecutor::DExecute() {
  LOG_INFO("Hash Executor");

  if (!done_) {
    const planner::HashPlan &node = GetPlanNode<planner::HashPlan>();

    while (children_[0]->Execute()) {
      child_tiles_.emplace_back(children_[0]->GetOutput());
    }

    if (child_tiles_.size() == 0) return false;


    /* HashKeys should be a vector of TupleValue expr
     * from which we construct a vector of column ids that represent the
     * attributes of the underlying table.
     * The hash table builds on these attributes */
    auto &hashkeys = node.GetHashKeys();
    for (auto &hashkey : hashkeys) {
      assert(hashkey->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE);
      auto tuple_value = reinterpret_cast<const expression::TupleValueExpression *>(hashkey.get());
      //assert(tuple_value->GetTupleIdx() == 0);
      column_ids_.push_back(tuple_value->GetColumnId());
    }

    for (size_t i = 0; i < child_tiles_.size(); i++) {
      auto tile = child_tiles_[i].get();
      for (oid_t tuple_id : *tile) {
        htable_[HashMapType::key_type(tile, tuple_id, &column_ids_)].insert(std::make_pair(i, tuple_id));
      }
    }

    done_ = true;
  }

  DumpHashTable();

  while (result_itr < child_tiles_.size()) {
    if (child_tiles_[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(child_tiles_[result_itr++].release());
      return true;
    }
  }
  return false;
}


void HashExecutor::DumpHashTable() const {
  assert(done_);
  for (auto &kv : htable_) {
    LOG_INFO("Key %lu, Num of tuple: %lu", kv.first.HashCode(), kv.second.size());
  }

}

} /* namespace executor */
} /* namespace peloton */
