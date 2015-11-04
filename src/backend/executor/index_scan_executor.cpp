//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_scan_executor.cpp
//
// Identification: src/backend/executor/index_scan_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
IndexScanExecutor::IndexScanExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class Dinit() first, then do my job.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  assert(children_.size() == 0);

  // Grab info from plan node and check it
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  index_ = node.GetIndex();
  assert(index_ != nullptr);

  result_itr = START_OID;
  done_ = false;

  column_ids_ = node.GetColumnIds();
  key_column_ids_ = node.GetKeyColumnIds();
  expr_types_ = node.GetExprTypes();
  values_ = node.GetValues();
  runtime_keys_ = node.GetRunTimeKeys();
  predicate_ = node.GetPredicate();

  if (runtime_keys_.size() != 0) {
    assert(runtime_keys_.size() == values_.size());

    if (!key_ready) {
      values_.clear();

      for (auto expr : runtime_keys_) {
        auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
        LOG_INFO("Evaluated runtime scan key: %s", value.Debug().c_str());
        values_.push_back(value);
      }

      key_ready = true;
    }
  }

  table_ = node.GetTable();

  if (table_ != nullptr) {
      full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
      std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
  }

  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {
  LOG_INFO("Index Scan executor :: 0 child");

  if (!done_) {
    auto status = ExecIndexLookup();
    if (status == false) return false;
    ExecPredication();
    ExecProjection();
  }

  // Already performed the index lookup
  assert(done_);

  while (result_itr < result.size()) {  // Avoid returning empty tiles
    if (result[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }

  }  // end while

  return false;
}

void IndexScanExecutor::ExecPredication() {
  if (nullptr == predicate_)
    return;
  unsigned int removed_count = 0;
  for (auto tile : result) {
    for (auto tuple_id : *tile) {
        expression::ContainerTuple<LogicalTile> tuple(tile, tuple_id);
        if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
                .IsFalse()) {
          removed_count++;
          tile->RemoveVisibility(tuple_id);
        }
    }
  }
  LOG_INFO("predicate removed %d row", removed_count);
}

void IndexScanExecutor::ExecProjection() {

  if (column_ids_.size() == 0)
    return;

  for (auto tile : result) {
    tile->ProjectColumns(full_column_ids_, column_ids_);
  }

}

bool IndexScanExecutor::ExecIndexLookup() {
  assert(!done_);

  std::vector<ItemPointer> tuple_locations;

  if (0 == key_column_ids_.size()) {
    tuple_locations = index_->Scan();
  } else {
    tuple_locations = index_->Scan(values_, key_column_ids_, expr_types_);
  }

  LOG_INFO("Tuple_locations.size(): %lu", tuple_locations.size());

  if (tuple_locations.size() == 0) return false;

  auto transaction_ = executor_context_->GetTransaction();
  txn_id_t txn_id = transaction_->GetTransactionId();
  cid_t commit_id = transaction_->GetLastCommitId();

  // Get the logical tiles corresponding to the given tuple locations
  result = LogicalTileFactory::WrapTileGroups(tuple_locations, full_column_ids_,
                                              txn_id, commit_id);

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result.size());

  return true;
}

}  // namespace executor
}  // namespace peloton
