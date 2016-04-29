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

  std::unique_ptr<Checkpoint> GetCheckpointer();

  // configuration
  void Configure(CheckpointType checkpoint_type, bool test_mode = false,
                 int num_checkpointers = 1) {
    checkpoint_type_ = checkpoint_type;
    test_mode_ = test_mode;
    num_checkpointers_ = num_checkpointers;
  }

 private:
  CheckpointManager();
  ~CheckpointManager() {}

  // static configurations for logging
  CheckpointType checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  // whether run checkpoint in test mode
  bool test_mode_ = false;

  // to be used in the future
  unsigned int num_checkpointers_ = 1;

  // the status of checkpoint manager
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
