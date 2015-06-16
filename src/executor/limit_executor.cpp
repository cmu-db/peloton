/**
 * @brief Executor for Limit node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/limit_executor.h"

#include "common/logger.h"
#include "common/types.h"
#include "executor/logical_tile.h"
#include "planner/limit_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor
 * @param node  LimitNode plan node corresponding to this executor
 */
LimitExecutor::LimitExecutor(planner::AbstractPlanNode *node, Transaction *transaction)
  : AbstractExecutor(node, transaction){
}

bool LimitExecutor::DInit(){
  assert(children_.size() == 1);
  return true;
}

bool LimitExecutor::DExecute(){
  assert(children_.size() == 1);

  // Grab data from plan node
  const planner::LimitNode &node = GetNode<planner::LimitNode>();
  const size_t limit = node.GetLimit();
  const size_t offset = node.GetOffset();

  LOG_TRACE("Limit executor \n");

  while(num_returned_ < limit && children_[0]->Execute()){
    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());
    for(oid_t tuple_id : *tile){
      if(num_skipped_ < offset){
        tile->InvalidateTuple(tuple_id);  // "below" tuples
        num_skipped_++;
      }
      else if(num_returned_ < limit){
        num_returned_++;  // good tuples
      }
      else{
        tile->InvalidateTuple(tuple_id);  // "above" tuples
      }
    }
    // Avoid returning empty tiles
    if(tile->GetTupleCount() > 0){
      SetOutput(tile.release());
      return true;
    }
  }

  return false;

//  if(!children_[0]->Execute() || num_returned_ == limit){
//    return false;
//  }
//
//  std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());
//
//  // TODO Naive implementation. Can be optimized.
//  // TODO Now it's possible to return a tile with no valid tuples,
//  // need to improve it.
//  for(oid_t tuple_id : *tile){
//    if(num_skipped_ < offset){
//      tile->InvalidateTuple(tuple_id);  // "below" tuples
//      num_skipped_++;
//    }
//    else if(num_returned_ < limit){
//      num_returned_++;  // good tuples
//    }
//    else{
//      tile->InvalidateTuple(tuple_id);  // "above" tuples
//    }
//  }
//
//  SetOutput(tile.release());
//
//  return true;
}


} /* namespace executor */
} /* namespace nstore */
