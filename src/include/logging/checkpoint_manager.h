//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.h
//
// Identification: src/include/logging/checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>
#include <condition_variable>

#include "logging/checkpoint.h"
#include "common/logger.h"

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

  // start standby mode for checkpointers
  void StartStandbyMode();

  // Check whether the checkpointer is in checkpointing mode
  inline bool IsInCheckpointingMode() {
    return (checkpoint_status_ == CHECKPOINT_STATUS_CHECKPOINTING);
  }

  // start recovery mode for checkpointers
  void StartRecoveryMode();

  // Checkpoint status
  void SetCheckpointStatus(CheckpointStatus checkpoint_status);

  CheckpointStatus GetCheckpointStatus();

  // get a checkpointer by index
  Checkpoint *GetCheckpointer(unsigned int idx);

  // initialize a list of checkpointers
  void InitCheckpointers();

  // configuration
  void Configure(CheckpointType checkpoint_type,
                 bool disable_file_access = false, int num_checkpointers = 1) {
    checkpoint_type_ = checkpoint_type;
    disable_file_access_ = disable_file_access;
    num_checkpointers_ = num_checkpointers;
  }

  // remove all checkpointers
  void DestroyCheckpointers();

  void SetRecoveredCid(cid_t recovered_cid);

  cid_t GetRecoveredCid();

 private:
  CheckpointManager();
  ~CheckpointManager() {}

  // static configurations for logging
  CheckpointType checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  // mainly used for testing
  bool disable_file_access_ = false;

  // to be used in the future
  unsigned int num_checkpointers_ = 1;

  // the status of checkpoint manager
  CheckpointStatus checkpoint_status_ = CHECKPOINT_STATUS_INVALID;

  cid_t recovered_cid_ = 0;

  // used for multiple checkpointer
  // std::atomic<unsigned int> status_change_count_;

  // To synch the status
  std::mutex checkpoint_status_mutex_;
  std::condition_variable checkpoint_status_cv_;

  std::vector<std::unique_ptr<Checkpoint>> checkpointers_;
};
}
}
