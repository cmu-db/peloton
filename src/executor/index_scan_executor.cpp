/**
 * @brief Executor for index scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "expression/container_tuple.h"
#include "planner/index_scan_node.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

#include "common/logger.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(planner::AbstractPlanNode *node, Transaction *transaction)
: AbstractExecutor(node, transaction), result_itr(0), done(false) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  assert(children_.size() == 0);
  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {
  assert(children_.size() == 0);

  LOG_TRACE("Index Scan executor :: 0 child \n");

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

  // Grab info from plan node.
  const planner::IndexScanNode &node = GetNode<planner::IndexScanNode>();
  const index::Index *index = node.GetIndex();
  const std::vector<oid_t> &column_ids = node.GetColumnIds();

  assert(index != nullptr);
  assert(column_ids.size() > 0);

  const storage::Tuple* start_key = node.GetStartKey();
  const storage::Tuple* end_key = node.GetEndKey();
  bool start_inclusive = node.IsStartInclusive();
  bool end_inclusive = node.IsEndInclusive();
  std::vector<ItemPointer> tuple_locations;

  if(start_key == nullptr && end_key == nullptr) {
    return false;
  }
  else if(start_key == nullptr) {
    // < END_KEY
    if(end_inclusive == false) {
      tuple_locations = index->GetLocationsForKeyLT(end_key);
    }
    // <= END_KEY
    else {
      tuple_locations = index->GetLocationsForKeyLTE(end_key);
    }
  }
  else if(end_key == nullptr) {
    // > START_KEY
    if(start_inclusive == false) {
      tuple_locations = index->GetLocationsForKeyGT(start_key);
    }
    // >= START_KEY
    else {
      tuple_locations = index->GetLocationsForKeyGTE(start_key);
    }
  }
  else {
    // START_KEY < .. < END_KEY
    tuple_locations = index->GetLocationsForKeyBetween(start_key, end_key);
  }

  LOG_TRACE("Tuple locations : %lu \n", tuple_locations.size());

  if(tuple_locations.size() == 0)
    return false;

  txn_id_t txn_id = transaction_->GetTransactionId();
  cid_t commit_id = transaction_->GetLastCommitId();

  // Get the logical tiles corresponding to the given tuple locations
  result = LogicalTileFactory::WrapTupleLocations(tuple_locations, column_ids,
                                                  txn_id, commit_id);
  done = true;

  LOG_TRACE("Result tiles : %lu \n", result.size());

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

} // namespace executor
} // namespace nstore
