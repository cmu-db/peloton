//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.cpp
//
// Identification: src/executor/index_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/index_scan_executor.h"

#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "index/index.h"
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"
#include "storage/masked_tuple.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

IndexScanExecutor::~IndexScanExecutor() {
  // Nothing to do here
}

/**
 * @brief Let base class Dinit() first, then do my job.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  PL_ASSERT(children_.size() == 0);

  // Grab info from plan node and check it
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  index_ = node.GetIndex();
  PL_ASSERT(index_ != nullptr);

  index_predicate_ = node.GetIndexPredicate();

  result_itr_ = START_OID;
  result_.clear();
  done_ = false;
  key_ready_ = false;

  column_ids_ = node.GetColumnIds();
  key_column_ids_ = node.GetKeyColumnIds();
  expr_types_ = node.GetExprTypes();
  values_ = node.GetValues();
  runtime_keys_ = node.GetRunTimeKeys();
  predicate_ = node.GetPredicate();
  left_open_ = node.GetLeftOpen();
  right_open_ = node.GetRightOpen();

  // This is for limit operation accelerate
  limit_ = node.GetLimit();
  limit_number_ = node.GetLimitNumber();
  limit_offset_ = node.GetLimitOffset();
  descend_ = node.GetDescend();

  if (runtime_keys_.size() != 0) {
    PL_ASSERT(runtime_keys_.size() == values_.size());

    if (!key_ready_) {
      values_.clear();

      for (auto expr : runtime_keys_) {
        auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
        LOG_TRACE("Evaluated runtime scan key: %s", value.GetInfo().c_str());
        values_.push_back(value.Copy());
      }

      key_ready_ = true;
    }
  }

  table_ = node.GetTable();

  // PAVLO (2017-01-05): This seems unnecessary and a waste of time
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
  LOG_TRACE("Index Scan executor :: 0 child");

  if (!done_) {
    if (index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
      auto status = ExecPrimaryIndexLookup();
      if (status == false) return false;
    } else {
      auto status = ExecSecondaryIndexLookup();
      if (status == false) return false;
    }
  }
  // Already performed the index lookup
  PL_ASSERT(done_);

  while (result_itr_ < result_.size()) {  // Avoid returning empty tiles
    if (result_[result_itr_]->GetTupleCount() == 0) {
      result_itr_++;
      continue;
    } else {
      LOG_TRACE("Information %s", result_[result_itr_]->GetInfo().c_str());
      SetOutput(result_[result_itr_]);
      result_itr_++;
      return true;
    }

  }  // end while

  return false;
}

bool IndexScanExecutor::ExecPrimaryIndexLookup() {
  PL_ASSERT(!done_);

  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  PL_ASSERT(index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    // Limit clause accelerate
    if (limit_) {
      // invoke index scan limit
      if (!descend_) {
        LOG_TRACE("ASCENDING SCAN LIMIT in Primary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_,
                          ScanDirectionType::FORWARD, tuple_location_ptrs,
                          &index_predicate_.GetConjunctionList()[0],
                          limit_number_, limit_offset_);
      } else {
        LOG_TRACE("DESCENDING SCAN LIMIT in Primary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_,
                          ScanDirectionType::BACKWARD, tuple_location_ptrs,
                          &index_predicate_.GetConjunctionList()[0],
                          limit_number_, limit_offset_);

        LOG_TRACE("1-Result size is %lu", result_.size());
      }
    }
    // Normal SQL (without limit)
    else {
      LOG_TRACE("Index Scan in Primary Index");
      index_->Scan(values_, key_column_ids_, expr_types_,
                   ScanDirectionType::FORWARD, tuple_location_ptrs,
                   &index_predicate_.GetConjunctionList()[0]);
    }

    LOG_TRACE("tuple_location_ptrs:%lu", tuple_location_ptrs.size());
  }

  if (tuple_location_ptrs.size() == 0) {
    LOG_TRACE("no tuple is retrieved from index.");
    return false;
  }

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();
  auto &manager = catalog::Manager::GetInstance();
  std::vector<ItemPointer> visible_tuple_locations;
  std::map<oid_t, std::vector<oid_t>> visible_tuples;

#ifdef LOG_TRACE_ENABLED
  int num_tuples_examined = 0;
#endif

  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();
    size_t chain_length = 0;

#ifdef LOG_TRACE_ENABLED
    num_tuples_examined++;
#endif
    // the following code traverses the version chain until a certain visible
    // version is found.
    // we should always find a visible version from a version chain.
    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, tuple_location.offset);

      // if the tuple is deleted
      if (visibility == VisibilityType::DELETED) {
        LOG_TRACE("encounter deleted tuple: %u, %u", tuple_location.block,
                  tuple_location.offset);
        break;
      }
      // if the tuple is visible.
      else if (visibility == VisibilityType::OK) {
        LOG_TRACE("perform read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        bool eval = true;
        // if having predicate, then perform evaluation.
        if (predicate_ != nullptr) {
          LOG_TRACE("perform predicate evaluate");
          ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                   tuple_location.offset);
          eval =
              predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
        }
        // if passed evaluation, then perform write.
        if (eval == true) {
          LOG_TRACE("perform read operation");
          auto res = transaction_manager.PerformRead(
              current_txn, tuple_location, acquire_owner);
          if (!res) {
            LOG_TRACE("read nothing");
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            return res;
          }
          // if perform read is successful, then add to visible tuple vector.
          visible_tuple_locations.push_back(tuple_location);
        }

        break;
      }
      // if the tuple is not visible.
      else {
        PL_ASSERT(visibility == VisibilityType::INVISIBLE);

        LOG_TRACE("Invisible read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        bool is_acquired = (tile_group_header->GetTransactionId(
                                tuple_location.offset) == INITIAL_TXN_ID);
        bool is_alive =
            (tile_group_header->GetEndCommitId(tuple_location.offset) <=
             current_txn->GetReadId());
        if (is_acquired && is_alive) {
          // See an invisible version that does not belong to any one in the
          // version chain.
          // this means that some other transactions have modified the version
          // chain.
          // Wire back because the current version is expired. have to search
          // from scratch.
          tuple_location =
              *(tile_group_header->GetIndirection(tuple_location.offset));
          tile_group = manager.GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          chain_length = 0;
          continue;
        }

        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);

        // there must exist a visible version.
        if (tuple_location.IsNull()) {
          if (chain_length == 1) {
            break;
          }

          // in most cases, there should exist a visible version.
          // if we have traversed through the chain and still can not fulfill
          // one of the above conditions,
          // then return result_failure.
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        // search for next version.
        tile_group = manager.GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
        continue;
      }
    }
    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
  }
  LOG_TRACE("Examined %d tuples from index %s", num_tuples_examined,
            index_->GetName().c_str());

  LOG_TRACE("%ld tuples before pruning boundaries",
            visible_tuple_locations.size());

  // Check whether the boundaries satisfy the required condition
  CheckOpenRangeWithReturnedTuples(visible_tuple_locations);

  LOG_TRACE("%ld tuples after pruning boundaries",
            visible_tuple_locations.size());

  for (auto &visible_tuple_location : visible_tuple_locations) {
    visible_tuples[visible_tuple_location.block]
        .push_back(visible_tuple_location.offset);
  }

  // Construct a logical tile for each block
  for (auto tuples : visible_tuples) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuples.first);

    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples.second));
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }

    result_.push_back(logical_tile.release());
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}

bool IndexScanExecutor::ExecSecondaryIndexLookup() {
  LOG_TRACE("ExecSecondaryIndexLookup");
  PL_ASSERT(!done_);
  PL_ASSERT(index_->GetIndexType() != IndexConstraintType::PRIMARY_KEY);

  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    // Limit clause accelerate
    if (limit_) {
      // invoke index scan limit
      if (!descend_) {
        LOG_TRACE("ASCENDING SCAN LIMIT in Secondary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_,
                          ScanDirectionType::FORWARD, tuple_location_ptrs,
                          &index_predicate_.GetConjunctionList()[0],
                          limit_number_, limit_offset_);
      } else {
        LOG_TRACE("DESCENDING SCAN LIMIT in Secondary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_,
                          ScanDirectionType::BACKWARD, tuple_location_ptrs,
                          &index_predicate_.GetConjunctionList()[0],
                          limit_number_, limit_offset_);

        if (tuple_location_ptrs.size() == 0) {
          LOG_TRACE("2-Result size is %lu", tuple_location_ptrs.size());
        }
      }
    }
    // Normal SQL (without limit)
    else {
      LOG_TRACE("Index Scan in Primary Index");
      index_->Scan(values_, key_column_ids_, expr_types_,
                   ScanDirectionType::FORWARD, tuple_location_ptrs,
                   &index_predicate_.GetConjunctionList()[0]);
    }
  }

  if (tuple_location_ptrs.size() == 0) {
    LOG_TRACE("no tuple is retrieved from index.");
    return false;
  }

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  std::vector<ItemPointer> visible_tuple_locations;
  std::map<oid_t, std::vector<oid_t>> visible_tuples;
  auto &manager = catalog::Manager::GetInstance();

  // Quickie Hack
  // Sometimes we can get the tuples we need in the same block if they
  // were inserted at the same time. So we'll record the last block that
  // we got for each tuple and check whether its the same to avoid having
  // to go back to the catalog each time.
  oid_t last_block = INVALID_OID;
  std::shared_ptr<storage::TileGroup> tile_group;
  storage::TileGroupHeader *tile_group_header = nullptr;

#ifdef LOG_TRACE_ENABLED
  int num_tuples_examined = 0;
  int num_blocks_reused = 0;
#endif

  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;
    if (tuple_location.block != last_block) {
      tile_group = manager.GetTileGroup(tuple_location.block);
      tile_group_header = tile_group.get()->GetHeader();
    }
#ifdef LOG_TRACE_ENABLED
    else
      num_blocks_reused++;
    num_tuples_examined++;
#endif

    // the following code traverses the version chain until a certain visible
    // version is found.
    // we should always find a visible version from a version chain.
    // different from primary key index lookup, we have to compare the
    // secondary
    // key to guarantee the correctness of the result.
    size_t chain_length = 0;
    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, tuple_location.offset);

      // if the tuple is deleted
      if (visibility == VisibilityType::DELETED) {
        LOG_TRACE("encounter deleted tuple: %u, %u", tuple_location.block,
                  tuple_location.offset);
        break;
      }
      // if the tuple is visible.
      else if (visibility == VisibilityType::OK) {
        LOG_TRACE("perform read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        // Further check if the version has the secondary key
        ContainerTuple<storage::TileGroup> candidate_tuple(tile_group.get(),
            tuple_location.offset);

        LOG_TRACE("candidate_tuple size: %s",
                  candidate_tuple.GetInfo().c_str());
        // Construct the key tuple
        auto &indexed_columns = index_->GetKeySchema()->GetIndexedColumns();
        storage::MaskedTuple key_tuple(&candidate_tuple, indexed_columns);

        // Compare the key tuple and the key
        if (index_->Compare(key_tuple, key_column_ids_, expr_types_, values_) ==
            false) {
          LOG_TRACE("Secondary key mismatch: %u, %u\n", tuple_location.block,
                    tuple_location.offset);
          break;
        }

        bool eval = true;
        // if having predicate, then perform evaluation.
        if (predicate_ != nullptr) {
          eval = predicate_->Evaluate(&candidate_tuple, nullptr,
                                      executor_context_).IsTrue();
        }
        // if passed evaluation, then perform write.
        if (eval == true) {
          auto res = transaction_manager.PerformRead(
              current_txn, tuple_location, acquire_owner);
          if (!res) {
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            LOG_TRACE("passed evaluation, but txn read fails");
            return res;
          }
          // if perform read is successful, then add to visible tuple vector.
          visible_tuple_locations.push_back(tuple_location);
          LOG_TRACE("passed evaluation, visible_tuple_locations size: %lu",
                    visible_tuple_locations.size());
        } else {
          LOG_TRACE("predicate evaluate fails");
        }

        break;
      }
      // if the tuple is not visible.
      else {
        PL_ASSERT(visibility == VisibilityType::INVISIBLE);

        LOG_TRACE("Invisible read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        bool is_acquired = (tile_group_header->GetTransactionId(
                                tuple_location.offset) == INITIAL_TXN_ID);
        bool is_alive =
            (tile_group_header->GetEndCommitId(tuple_location.offset) <=
             current_txn->GetReadId());
        if (is_acquired && is_alive) {
          // See an invisible version that does not belong to any one in the
          // version chain.
          // this means that some other transactions have modified the version
          // chain.
          // Wire back because the current version is expired. have to search
          // from scratch.
          tuple_location =
              *(tile_group_header->GetIndirection(tuple_location.offset));
          tile_group = manager.GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          chain_length = 0;
          continue;
        }

        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);

        if (tuple_location.IsNull()) {
          // For an index scan on a version chain, the result should be one of
          // the following:
          //    (1) find a visible version
          //    (2) find a deleted version
          //    (3) find an aborted version with chain length equal to one
          if (chain_length == 1) {
            break;
          }

          // in most cases, there should exist a visible version.
          // if we have traversed through the chain and still can not fulfill
          // one of the above conditions,
          // then return result_failure.
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        // search for next version.
        tile_group = manager.GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
      }
    }
    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
  }
  LOG_TRACE("Examined %d tuples from index %s [num_blocks_reused=%d]",
            num_tuples_examined, index_->GetName().c_str(), num_blocks_reused);

  // Check whether the boundaries satisfy the required condition
  CheckOpenRangeWithReturnedTuples(visible_tuple_locations);

  for (auto &visible_tuple_location : visible_tuple_locations) {
    visible_tuples[visible_tuple_location.block]
        .push_back(visible_tuple_location.offset);
  }

  // Construct a logical tile for each block
  for (auto tuples : visible_tuples) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuples.first);

    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples.second));
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }

    result_.push_back(logical_tile.release());
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}

void IndexScanExecutor::CheckOpenRangeWithReturnedTuples(
    std::vector<ItemPointer> &tuple_locations) {
  while (left_open_) {
    LOG_TRACE("Range left open!");
    auto tuple_location_itr = tuple_locations.begin();

    if (tuple_location_itr == tuple_locations.end() ||
        CheckKeyConditions(*tuple_location_itr) == true)
      left_open_ = false;
    else
      tuple_locations.erase(tuple_location_itr);
  }

  while (right_open_) {
    LOG_TRACE("Range right open!");
    auto tuple_location_itr = tuple_locations.rbegin();

    if (tuple_location_itr == tuple_locations.rend() ||
        CheckKeyConditions(*tuple_location_itr) == true)
      right_open_ = false;
    else
      tuple_locations.pop_back();
  }
}

bool IndexScanExecutor::CheckKeyConditions(const ItemPointer &tuple_location) {
  // The size of these three arrays must be the same
  PL_ASSERT(key_column_ids_.size() == expr_types_.size());
  PL_ASSERT(expr_types_.size() == values_.size());

  LOG_TRACE("Examining key conditions for the returned tuple.");

  auto &manager = catalog::Manager::GetInstance();

  auto tile_group = manager.GetTileGroup(tuple_location.block);
  ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                           tuple_location.offset);

  // This is the end of loop
  oid_t cond_num = key_column_ids_.size();

  // Go over each attribute in the list of comparison columns
  // The key_columns_ids, as the name shows, saves the key column ids that
  // have values and expression needs to be compared.

  for (oid_t i = 0; i < cond_num; i++) {
    // First retrieve the tuple column ID from the map, and then map
    // it to the column ID of index key
    oid_t tuple_key_column_id = key_column_ids_[i];

    // This the comparison right hand side operand
    const type::Value &rhs = values_[i];

    // Also retrieve left hand side operand using index key column ID
    type::Value val = (tuple.GetValue(tuple_key_column_id));
    const type::Value &lhs = val;

    // Expression type. We use this to interpret comparison result
    //
    // Possible results of comparison are: EQ, >, <
    const ExpressionType expr_type = expr_types_[i];

    // If the operation is IN, then use the boolean values comparator
    // that determines whether a value is in a list
    //
    // To make the procedure more uniform, we interpret IN as EQUAL
    // and NOT IN as NOT EQUAL, and react based on expression type below
    // accordingly
    /*if (expr_type == ExpressionType::COMPARE_IN) {
      bool bret = lhs.InList(rhs);

      if (bret == true) {
        diff = VALUE_COMPARE_EQUAL;
      } else {
        diff = VALUE_COMPARE_NO_EQUAL;
      }
    } else {
      diff = lhs.Compare(rhs);
    }

    LOG_TRACE("Difference : %d ", diff);*/
    if (lhs.CompareEquals(rhs) == CmpBool::TRUE) {
      switch (expr_type) {
        case ExpressionType::COMPARE_EQUAL:
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        case ExpressionType::COMPARE_IN:
          continue;

        case ExpressionType::COMPARE_NOTEQUAL:
        case ExpressionType::COMPARE_LESSTHAN:
        case ExpressionType::COMPARE_GREATERTHAN:
          return false;

        default:
          throw IndexException("Unsupported expression type : " +
                               ExpressionTypeToString(expr_type));
      }
    } else {
      if (lhs.CompareLessThan(rhs) == CmpBool::TRUE) {
        switch (expr_type) {
          case ExpressionType::COMPARE_NOTEQUAL:
          case ExpressionType::COMPARE_LESSTHAN:
          case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            continue;

          case ExpressionType::COMPARE_EQUAL:
          case ExpressionType::COMPARE_GREATERTHAN:
          case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          case ExpressionType::COMPARE_IN:
            return false;

          default:
            throw IndexException("Unsupported expression type : " +
                                 ExpressionTypeToString(expr_type));
        }
      } else {
        if (lhs.CompareGreaterThan(rhs) == CmpBool::TRUE) {
          switch (expr_type) {
            case ExpressionType::COMPARE_NOTEQUAL:
            case ExpressionType::COMPARE_GREATERTHAN:
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
              continue;

            case ExpressionType::COMPARE_EQUAL:
            case ExpressionType::COMPARE_LESSTHAN:
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            case ExpressionType::COMPARE_IN:
              return false;

            default:
              throw IndexException("Unsupported expression type : " +
                                   ExpressionTypeToString(expr_type));
          }
        } else {
          // Since it is an AND predicate, we could directly return false
          return false;
        }
      }
    }
  }

  LOG_TRACE("Examination returning true.");

  return true;
}

// column_ids is the right predicate column id. For example,
// i_id = s_id, then s_id is column_ids
// But the passing value is the result (output) id. We need to transform it to
// physical id
void IndexScanExecutor::UpdatePredicate(
    const std::vector<oid_t> &column_ids,
    const std::vector<type::Value> &values) {
  // Update index predicate
  LOG_TRACE("values_ size %lu", values_.size());

  std::vector<oid_t> key_column_ids;

  PL_ASSERT(column_ids.size() <= column_ids_.size());
  // Get the real physical ids
  for (auto column_id : column_ids) {
    key_column_ids.push_back(column_ids_[column_id]);

    LOG_TRACE("Output id is %d---physical column id is %d", column_id,
              column_ids_[column_id]);
  }

  // Update values in index plan node
  PL_ASSERT(key_column_ids.size() == values.size());
  PL_ASSERT(key_column_ids_.size() == values_.size());

  // Find out the position (offset) where is key_column_id
  for (oid_t new_idx = 0; new_idx < key_column_ids.size(); new_idx++) {
    unsigned int current_idx = 0;
    for (; current_idx < values_.size(); current_idx++) {
      if (key_column_ids[new_idx] == key_column_ids_[current_idx]) {
        LOG_TRACE("Orignial is %d:%s", key_column_ids[new_idx],
                  values_[current_idx].GetInfo().c_str());
        LOG_TRACE("Changed to %d:%s", key_column_ids[new_idx],
                  values[new_idx].GetInfo().c_str());
        values_[current_idx] = values[new_idx];

        // There should not be two same columns. So when we find a column, we
        // should break the loop
        break;
      }
    }

    // If new value doesn't exist in current value list, add it.
    // For the current simple optimizer, since all the key column ids must be
    // initiated when creating index_scan_plan, we don't need to examine
    // whether
    // the passing column and value exist or not (they definitely exist). But
    // for the future optimizer, we probably change the logic. So we still
    // keep
    // the examine code here.
    if (current_idx == values_.size()) {
      LOG_TRACE("Add new column for index predicate:%u-%s",
                key_column_ids[new_idx], values[new_idx].GetInfo().c_str());

      // Add value
      values_.push_back(values[new_idx]);

      // Add column id
      key_column_ids_.push_back(key_column_ids[new_idx]);

      // Add column type.
      // TODO: We should add other types in the future
      expr_types_.push_back(ExpressionType::COMPARE_EQUAL);
    }
  }

  // Update the new value
  index_predicate_.GetConjunctionListToSetup()[0]
      .SetTupleColumnValue(index_.get(), key_column_ids, values);
}

void IndexScanExecutor::ResetState() {
  result_.clear();

  result_itr_ = START_OID;

  done_ = false;

  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  left_open_ = node.GetLeftOpen();

  right_open_ = node.GetRightOpen();
}

}  // namespace executor
}  // namespace peloton
