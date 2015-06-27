/**
 * @brief append executor.
 *
 * Copyright(c) 2015, CMU
 */


#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/append_executor.h"
#include "backend/planner/append_node.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
AppendExecutor::AppendExecutor(planner::AbstractPlanNode *node,
                               concurrency::Transaction *transaction)
: AbstractExecutor(node, transaction) {

}

/**
 * @brief Basic checks.
 * @return true on success, false otherwise.
 */
bool AppendExecutor::DInit() {
  // should have >= 2 children, otherwise pointless.
  assert(children_.size() >= 2);
  assert(cur_child_id_ == 0);

  return true;
}

bool AppendExecutor::DExecute(){
  LOG_TRACE("Append executor \n");

  while(cur_child_id_ < children_.size()) {
    if(children_[cur_child_id_]->Execute()){
      SetOutput(children_[cur_child_id_]->GetOutput());
      return true;
    }
    else
      cur_child_id_++;
  }

  return false;
}



} /* namespace executor */
} /* namespace peloton */
