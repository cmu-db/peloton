/**
 * @brief hash-based set operation executor.
 *
 * Copyright(c) 2015, CMU
 */

#include <unordered_map>
#include <utility>
#include <vector>

#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/hash_set_op_executor.h"
#include "backend/planner/set_op_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor
 */
HashSetOpExecutor::HashSetOpExecutor(planner::AbstractPlanNode *node,
                                     concurrency::Transaction *transaction)
: AbstractExecutor(node, transaction) {

}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool HashSetOpExecutor::DInit(){
  assert(children_.size() == 2);
  assert(!hash_done_);
  assert(set_op_ == SETOP_TYPE_INVALID);

  return true;
}

bool HashSetOpExecutor::DExecute(){
  LOG_TRACE("Set Op executor \n");

  if(!hash_done_)
    ExecuteHelper();

  assert(hash_done_);

  // Avoid returning empty tiles
  while(next_tile_to_return_ < left_tiles_.size()){
    if(left_tiles_[next_tile_to_return_]->GetTupleCount() > 0){
      SetOutput(left_tiles_[next_tile_to_return_].release());
      next_tile_to_return_++;
      return true;
    }
    else
      next_tile_to_return_++;
  }
  return false;
}

bool HashSetOpExecutor::ExecuteHelper(){
  assert(children_.size() == 2);
  assert(!hash_done_);

  // Grab data from plan node
  const planner::SetOpNode &node = GetNode<planner::SetOpNode>();
  set_op_ = node.GetSetOp();
  assert(set_op_ != SETOP_TYPE_INVALID);

  size_t hint_num_groups = 100; // TODO: hard code at the moment

  // Extract all input from left child
  while(children_[0]->Execute()){
    left_tiles_.emplace_back(children_[0]->GetOutput());
  }

  if(left_tiles_.size() == 0)
    return false;

  // Define hash table key type
  typedef struct ht_key_t {
    LogicalTile* tile;
    oid_t tuple_id;

    ht_key_t(LogicalTile* t, oid_t tid)
    : tile(t), tuple_id(tid) {
    }

    bool operator==(const ht_key_t& rhs) const {
      return (rhs.tile == tile && rhs.tuple_id == tuple_id);
    };
  } ht_key_t;

  // Define mapped type
  typedef struct {
    size_t left = 0;
    size_t right = 0;
  }  counter_pair_t;

  // Prepare hash function object and equality test function object for hash table
  struct TupleHasher {
    TupleHasher(oid_t num_cols, size_t seed = 0)
    : num_cols_(num_cols),
      seed_(seed){
    }

    size_t operator()(const ht_key_t& key) const{
      size_t hval = seed_;
      for(oid_t col_id = 0; col_id < num_cols_; col_id++){
        Value value = key.tile->GetValue(key.tuple_id, col_id);
        value.HashCombine(hval);
      }
      return hval;
    }

   private:
    const oid_t num_cols_;
    const size_t seed_;
  };

  struct TupleComparer{
    TupleComparer(oid_t num_cols)
    : num_cols_(num_cols){
    }

    bool operator()(const ht_key_t& ta, const ht_key_t& tb) const{
      for(oid_t col_id = 0; col_id < num_cols_; col_id++){
        Value lval = ta.tile->GetValue(ta.tuple_id, col_id);
        Value rval = tb.tile->GetValue(tb.tuple_id, col_id);
        if(lval != rval)
          return false;
      }
      return true;
    }

   private:
    const oid_t num_cols_;
  };

  TupleHasher hf(left_tiles_[0]->GetColumnCount());
  TupleComparer comp(left_tiles_[0]->GetColumnCount());

  // Define the hash table
  std::unordered_map<ht_key_t, counter_pair_t, TupleHasher, TupleComparer> htable(hint_num_groups, hf, comp);

  // Scan the left child's input and update the counters
  for(auto &tile : left_tiles_){
    for(oid_t tuple_id : *tile){
      htable[ht_key_t(tile.get(), tuple_id)].left++;
    }
  }

  // Scan the right child's input and update counter when appropriate
  while(children_[1]->Execute()){
    // Each right tile can be destroyed after processing
    std::unique_ptr<LogicalTile> tile(children_[1]->GetOutput());

    for(oid_t tuple_id : *tile){
      auto it = htable.find(ht_key_t(tile.get(), tuple_id));
      // Do nothing if this key never appears in the left child
      if(it != htable.end()){
        it->second.right++;
      }
    }
  }

  // Calculate the output number for each key
  switch(set_op_){
    case SETOP_TYPE_INTERSECT:
      UpdateHashTable<SETOP_TYPE_INTERSECT>(htable);
      break;
    case SETOP_TYPE_INTERSECT_ALL:
      UpdateHashTable<SETOP_TYPE_INTERSECT_ALL>(htable);
      break;
    case SETOP_TYPE_EXCEPT:
      UpdateHashTable<SETOP_TYPE_EXCEPT>(htable);
      break;
    case SETOP_TYPE_EXCEPT_ALL:
      UpdateHashTable<SETOP_TYPE_EXCEPT_ALL>(htable);
      break;
    case SETOP_TYPE_INVALID:
      return false;
  }


  /*
   * A bit cumbersome here:
   * Since the first tuple of each group is used as the representative key
   * in the hash table,
   * we cannot invalidate it otherwise subsequent key comparison will fail
   * (due to safety check in LogicalTile::GetValue()).
   * Instead, we skip the first tuple and process it in a second round.
   */

  // 1st round
  for(auto &tile : left_tiles_){
    for(oid_t tuple_id : *tile){
      auto it = htable.find(ht_key_t(tile.get(), tuple_id));

      assert(it != htable.end());

      if(it->first == ht_key_t(tile.get(), tuple_id))
        continue;
      else if(it->second.left > 0)
        it->second.left--;
      else
        tile->InvalidateTuple(tuple_id);
    }
  }

  // 2nd round
  for(auto& item : htable) {
    assert(item.second.left == 1 || item.second.left == 0);
    if(item.second.left == 0) {
      item.first.tile->InvalidateTuple(item.first.tuple_id);
    }
  }

  hash_done_ = true;
  next_tile_to_return_ = 0;
  return true;
}

/**
 * Based on the set-op type,
 * calculate the number of output copies of each tuples
 * and store it in the left counter.
 */
template <SetOpType SETOP, class HT>
bool HashSetOpExecutor::UpdateHashTable(HT &htable){
  for(auto& item : htable){
    switch(SETOP){
      case SETOP_TYPE_INTERSECT:
        item.second.left = (item.second.right > 0) ? 1 : 0;
        break;
      case SETOP_TYPE_INTERSECT_ALL:
        item.second.left = std::min(item.second.left, item.second.right);
        break;
      case SETOP_TYPE_EXCEPT:
        item.second.left = (item.second.right > 0) ? 0 : 1;
        break;
      case SETOP_TYPE_EXCEPT_ALL:
        item.second.left = (item.second.left > item.second.right) ? (item.second.left - item.second.right) : 0;
        break;
      default:
        return false;
    }
  }
  return true;
}



} /* namespace executor */
} /* namespace nstore */
