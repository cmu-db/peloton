//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager.cpp
//
// Identification: src/gc/transaction_level_gc_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gc/tile_group_compactor.h"


namespace peloton {
namespace gc {

void TileGroupCompactor::CompactTileGroup(const oid_t &tile_group_id) {

  size_t attempts = 0;
  size_t max_attempts = 100;

  constexpr auto minPauseTime = std::chrono::microseconds(1);
  constexpr auto maxPauseTime = std::chrono::microseconds(100000);

  auto pause_time = minPauseTime;

  while (attempts < max_attempts) {

    auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
    if (tile_group == nullptr) {
      return; // this tile group no longer exists
    }

    storage::DataTable *table =
        dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());

    if (table == nullptr) {
      return; // this table no longer exists
    }

    bool success = MoveTuplesOutOfTileGroup(table, tile_group);

    if (success) {
      return;
    }

    // Otherwise, transaction failed, so we'll retry with exponential backoff
    std::this_thread::sleep_for(pause_time);
    pause_time = std::min(pause_time * 2, maxPauseTime);
  }
}

// Compacts tile group by moving all of its tuples to other tile groups
// Once empty, it will eventually get freed by the GCM
// returns true if txn succeeds or should not be retried, otherwise false
bool TileGroupCompactor::MoveTuplesOutOfTileGroup(
    storage::DataTable *table, std::shared_ptr<storage::TileGroup> tile_group) {

  auto tile_group_id = tile_group->GetTileGroupId();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn));

  auto tile_group_header = tile_group->GetHeader();
  PELOTON_ASSERT(tile_group_header != nullptr);

  // Construct Project Info (outside loop so only done once)
  TargetList target_list;
  DirectMapList direct_map_list;
  size_t num_columns = table->GetSchema()->GetColumnCount();

  for (size_t i = 0; i < num_columns; i++) {
    DirectMap direct_map = std::make_pair(i, std::make_pair(0, i));
    direct_map_list.push_back(direct_map);
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  // Update tuples in the given tile group
  for (oid_t physical_tuple_id = 0; physical_tuple_id < tile_group->GetAllocatedTupleCount(); physical_tuple_id++) {

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    auto visibility = txn_manager.IsVisible(
        txn, tile_group_header, physical_tuple_id,
        VisibilityIdType::COMMIT_ID);
    if (visibility != VisibilityType::OK) {
      // ignore garbage tuples because they don't prevent tile group freeing
      continue;
    }

    LOG_TRACE("Moving Physical Tuple id : %u ", physical_tuple_id);

    bool is_ownable = txn_manager.IsOwnable(
            txn, tile_group_header, physical_tuple_id);
    if (!is_ownable) {
      LOG_TRACE("Failed to move tuple. Not ownable.");
      txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
      txn_manager.AbortTransaction(txn);
      return false;
    }

    // if the tuple is not owned by any transaction and is visible to
    // current transaction, we'll try to move it to a new tile group
    bool acquired_ownership =
        txn_manager.AcquireOwnership(txn, tile_group_header,
                                                 physical_tuple_id);
    if (!acquired_ownership) {
      LOG_TRACE("Failed to move tuple. Could not acquire ownership of tuple.");
      txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
      txn_manager.AbortTransaction(txn);
      return false;
    }

    // check again now that we have ownsership
    // to ensure that this is stil the latest version
    bool is_latest_version = tile_group_header->GetPrevItemPointer(physical_tuple_id).IsNull();
    if (is_latest_version == false) {
      // if a tuple is not the latest version, then there's no point in moving it
      // this also does not conflict with our compaction operation, so don't abort
      LOG_TRACE("Skipping tuple, not latest version.");
      txn_manager.YieldOwnership(txn, tile_group_header,
                                         physical_tuple_id);
      continue;
    }

    ItemPointer new_location = table->AcquireVersion();
    PELOTON_ASSERT(new_location.IsNull() == false);

    auto &manager = catalog::Manager::GetInstance();
    auto new_tile_group = manager.GetTileGroup(new_location.block);

    ContainerTuple<storage::TileGroup> new_tuple(new_tile_group.get(),
                                                 new_location.offset);

    ContainerTuple<storage::TileGroup> old_tuple(tile_group.get(),
                                                 physical_tuple_id);
    
    project_info->Evaluate(&new_tuple, &old_tuple, nullptr,
                            executor_context.get());

    LOG_TRACE("perform move old location: %u, %u", old_location.block,
              old_location.offset);
    LOG_TRACE("perform move new location: %u, %u", new_location.block,
              new_location.offset);
    txn_manager.PerformUpdate(txn, old_location, new_location);
  }

  txn_manager.CommitTransaction(txn);
  return true;
}

}  // namespace gc
}  // namespace peloton




































