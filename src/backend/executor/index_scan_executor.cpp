/**
 * @brief Executor for index scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include "backend/executor/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"

#include "backend/common/logger.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(planner::AbstractPlanNode *node,
                                     ExecutorContext *executor_context)
: AbstractExecutor(node, executor_context) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  assert(children_.size() == 0);
  assert(executor_context_);

  LOG_TRACE("Index Scan executor :: 0 child \n");

  // Grab info from plan node and check it
  const planner::IndexScanNode &node = GetPlanNode<planner::IndexScanNode>();

  index_ = node.GetIndex();
  assert(index_ != nullptr);

  column_ids_ = node.GetColumnIds();
  assert(column_ids_.size() > 0);

  start_key_ = node.GetStartKey();
  end_key_ = node.GetEndKey();
  start_inclusive_ = node.IsStartInclusive();
  end_inclusive_ = node.IsEndInclusive();

  result_itr = START_OID;

  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {

  // Already performed the index lookup
  if(done) {
    if(result_itr == result.size()) {
      return false;
    }
    else {
      // Return appropriate tile and go to next tile
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }
  }

  // Else, need to do the index lookup
  std::vector<ItemPointer> tuple_locations;

  if(start_key_ == nullptr && end_key_ == nullptr) {
    return false;
  }
  else if(start_key_ == nullptr) {
    // < END_KEY
    if(end_inclusive_ == false) {
      tuple_locations = index_->GetLocationsForKeyLT(end_key_);
    }
    // <= END_KEY
    else {
      tuple_locations = index_->GetLocationsForKeyLTE(end_key_);
    }
  }
  else if(end_key_ == nullptr) {
    // > START_KEY
    if(start_inclusive_ == false) {
      tuple_locations = index_->GetLocationsForKeyGT(start_key_);
    }
    // >= START_KEY
    else {
      tuple_locations = index_->GetLocationsForKeyGTE(start_key_);
    }
  }
  else {
    // START_KEY < .. < END_KEY
    tuple_locations = index_->GetLocationsForKeyBetween(start_key_, end_key_);
  }

  LOG_TRACE("Tuple locations : %lu \n", tuple_locations.size());

  if(tuple_locations.size() == 0)
    return false;

  auto transaction_ = executor_context_->GetTransaction();
  txn_id_t txn_id = transaction_->GetTransactionId();
  cid_t commit_id = transaction_->GetLastCommitId();

  // Get the logical tiles corresponding to the given tuple locations
  result = LogicalTileFactory::WrapTileGroups(tuple_locations, column_ids_,
                                                  txn_id, commit_id);
  done = true;

  LOG_TRACE("Result tiles : %lu \n", result.size());

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

} // namespace executor
} // namespace peloton
