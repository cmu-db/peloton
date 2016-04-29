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

CheckpointManager::CheckpointManager() {
  Configure(peloton_checkpoint_mode, false, 1);
}

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
  auto checkpointer = GetCheckpointer();

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

std::unique_ptr<Checkpoint> CheckpointManager::GetCheckpointer() {
  return std::move(Checkpoint::GetCheckpoint(checkpoint_type_, test_mode_));
}

CheckpointStatus CheckpointManager::GetCheckpointStatus() {
  // Get the checkpoint status
  return checkpoint_status_;
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
