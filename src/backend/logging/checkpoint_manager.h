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

#pragma once
#include <vector>
#include <memory>

#include "backend/logging/checkpoint.h"
#include "backend/common/logger.h"

extern CheckpointType peloton_checkpoint_mode;

namespace peloton {
namespace logging {

class Checkpoint;

class CheckpointManager {
 public:
  CheckpointManager(const CheckpointManager &) = delete;
  CheckpointManager &operator=(const CheckpointManager &) = delete;
  CheckpointManager(CheckpointManager &&) = delete;
  CheckpointManager &operator=(CheckpointManager &&) = delete;

  // global singleton
  static CheckpointManager &GetInstance(void);

  void WaitForModeTransition(CheckpointStatus checkpoint_status, bool is_equal);

  void StartStandbyMode();

  // Check whether the checkpointer is in checkpointing mode
  inline bool IsInCheckpointingMode() {
    return (checkpoint_status_ == CHECKPOINT_STATUS_CHECKPOINTING);
  }

  void StartRecoveryMode();

  // Checkpoint status
  void SetCheckpointStatus(CheckpointStatus checkpoint_status);

  CheckpointStatus GetCheckpointStatus();

 private:
  CheckpointManager() {}
  ~CheckpointManager() {}

  CheckpointStatus checkpoint_status_ = CHECKPOINT_STATUS_INVALID;

  // used for multiple checkpointer
  // std::atomic<unsigned int> status_change_count_;

  // To synch the status
  std::mutex checkpoint_status_mutex_;
  std::condition_variable checkpoint_status_cv_;

  std::vector<std::unique_ptr<Checkpoint>> checkpointers_;
};
}
}
