/*-------------------------------------------------------------------------
 *
 * insert_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/insert_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/insert_executor.h"

#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "planner/insert_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(planner::AbstractPlanNode *node,
                               Context *context,
                               const std::vector<storage::Tuple *>& tuples)
: AbstractExecutor(node, context),
  tuples(tuples) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
  assert(children_.size() <= 1);
  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
  assert(children_.size() == 0);
  assert(context_);

  const planner::InsertNode &node = GetNode<planner::InsertNode>();

  // Insert given tuples into table

  for(auto tuple : tuples) {
    storage::Table *target_table = node.GetTable();
    id_t tuple_id = target_table->InsertTuple(context_->GetTransactionId(), tuple);

    if(tuple_id == INVALID_ID) {
      std::cout << "Insert failed \n";
      return false;
    }
  }


  return true;
}

} // namespace executor
} // namespace nstore
