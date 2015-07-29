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
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base calss Dinit() first, then do my job.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {

  auto status = AbstractScanExecutor::DInit();

  if(!status)
    return false;

  assert(children_.size() == 0);
  LOG_INFO("Index Scan executor :: 0 child \n");

  // Grab info from plan node and check it
  const planner::IndexScanNode &node = GetPlanNode<planner::IndexScanNode>();

  index_ = node.GetIndex();
  assert(index_ != nullptr);

  start_key_ = node.GetStartKey();
  end_key_ = node.GetEndKey();
  start_inclusive_ = node.IsStartInclusive();
  end_inclusive_ = node.IsEndInclusive();

  result_itr = START_OID;

  done_ = false;

  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {

  if(!done_){
    auto status = ExecIndexLookup();
    if(status == false)
      return false;
  }

  // Already performed the index lookup
  assert(done_);

  if (result_itr == result.size()) {
    return false;
  }
  else {
    // Return a tile
    // In order to be as lazy as possible, t
    // the generic predicate is checked here (instead of upfront)
    if(nullptr != predicate_){
      for (oid_t tuple_id : *result[result_itr]) {
        expression::ContainerTuple<LogicalTile> tuple(result[result_itr], tuple_id);
        if (predicate_->Evaluate(&tuple, nullptr, executor_context_).IsFalse()) {
          result[result_itr]->RemoveVisibility(tuple_id);
        }
      }
    }

    SetOutput(result[result_itr]);
    result_itr++;
    return true;
  }

}

bool IndexScanExecutor::ExecIndexLookup(){
  assert(!done_);

  std::vector<ItemPointer> tuple_locations;

  if (start_key_ == nullptr && end_key_ == nullptr) {
    return false;
  } else if (start_key_ == nullptr) {
    // < END_KEY
    if (end_inclusive_ == false) {
      tuple_locations = index_->GetLocationsForKeyLT(end_key_);
    }
    // <= END_KEY
    else {
      tuple_locations = index_->GetLocationsForKeyLTE(end_key_);
    }
  } else if (end_key_ == nullptr) {
    // > START_KEY
    if (start_inclusive_ == false) {
      tuple_locations = index_->GetLocationsForKeyGT(start_key_);
    }
    // >= START_KEY
    else {
      tuple_locations = index_->GetLocationsForKeyGTE(start_key_);
    }
  } else {
    // START_KEY < .. < END_KEY
    tuple_locations = index_->GetLocationsForKeyBetween(start_key_, end_key_);
  }

  LOG_INFO("Tuple locations : %lu \n", tuple_locations.size());

  if (tuple_locations.size() == 0) return false;

  auto transaction_ = executor_context_->GetTransaction();
  txn_id_t txn_id = transaction_->GetTransactionId();
  cid_t commit_id = transaction_->GetLastCommitId();

  // Get the logical tiles corresponding to the given tuple locations
  result = LogicalTileFactory::WrapTileGroups(tuple_locations, column_ids_,
                                              txn_id, commit_id);
  done_ = true;

  LOG_INFO("Result tiles : %lu \n", result.size());

  return true;
}

}  // namespace executor
}  // namespace peloton
