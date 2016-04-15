//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/checkpoint/simple_checkpoint.h"
#include "backend/logging/checkpoint_manager.h"

namespace peloton {
namespace logging {

CheckpointManager &CheckpointManager::GetInstance(void) {
  static CheckpointManager checkpoint_manager;
  return checkpoint_manager;
}

void CheckpointManager::WaitForModeTransition(
    CheckpointStatus checkpoint_status, bool is_equal) {
  // Wait for mode transition
  {
    std::unique_lock<std::mutex> wait_lock(checkpoint_status_mutex_);

    while ((!is_equal && checkpoint_status == checkpoint_status_) ||
           (is_equal && checkpoint_status != checkpoint_status_)) {
      checkpoint_status_cv_.wait(wait_lock);
    }
  }
}

void CheckpointManager::StartStandbyMode() {
  auto checkpointer = Checkpoint::GetCheckpoint(peloton_checkpoint_mode);

  // If checkpointer still doesn't exist, then we have disabled logging
  if (!checkpointer) {
    LOG_INFO("We have disabled checkpoint");
    return;
  }

  // Toggle status in log manager map
  SetCheckpointStatus(CHECKPOINT_STATUS_STANDBY);

  // Launch the checkpointer's main loop
  checkpointer->MainLoop();
}

void CheckpointManager::StartRecoveryMode() {
  // Toggle the status after STANDBY
  SetCheckpointStatus(CHECKPOINT_STATUS_RECOVERY);
}

CheckpointStatus CheckpointManager::GetCheckpointStatus() {
  // Get the checkpoint status
  return checkpoint_status_;
}

void CheckpointManager::PrepareRecovery() {
  auto &catalog_manager = catalog::Manager::GetInstance();
  // for all database
  auto db_count = catalog_manager.GetDatabaseCount();
  for (oid_t db_idx = 0; db_idx < db_count; db_idx++) {
    auto database = catalog_manager.GetDatabase(db_idx);
    // for all tables
    auto table_count = database->GetTableCount();
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      auto table = database->GetTable(table_idx);
      // drop existing tile groups
      table->DropTileGroups();
    }
  }
}

void CheckpointManager::SetCheckpointStatus(
    CheckpointStatus checkpoint_status) {
  {
    std::lock_guard<std::mutex> wait_lock(checkpoint_status_mutex_);

    // Set the status in the log manager map
    checkpoint_status_ = checkpoint_status;

    // notify everyone about the status change
    checkpoint_status_cv_.notify_all();
  }
}

}  // logging
}  // peloton
