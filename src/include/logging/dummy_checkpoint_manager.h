//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dummy_checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/dummy_checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/checkpoint_manager.h"

namespace peloton {
namespace logging {

class DummyCheckpointManager : public CheckpointManager {
  // Deleted functions
  DummyCheckpointManager(const DummyCheckpointManager &) = delete;
  DummyCheckpointManager &operator=(const DummyCheckpointManager &) = delete;
  DummyCheckpointManager(DummyCheckpointManager &&) = delete;
  DummyCheckpointManager &operator=(const DummyCheckpointManager &&) = delete;


public:
  DummyCheckpointManager() {}
  virtual ~DummyCheckpointManager() {}

  static DummyCheckpointManager& GetInstance() {
    static DummyCheckpointManager checkpoint_manager;
    return checkpoint_manager;
  }

  virtual void StartCheckpointing() {}
  virtual void StopCheckpointing() {}
  virtual void DoRecovery() {}

private:

  virtual void RecoverTable(storage::DataTable * UNUSED_ATTRIBUTE, const size_t &thread_id UNUSED_ATTRIBUTE, const cid_t &begin_cid UNUSED_ATTRIBUTE, FileHandle *file_handles UNUSED_ATTRIBUTE) {}

  virtual void CheckpointTable(storage::DataTable * UNUSED_ATTRIBUTE, const size_t &tile_group_count UNUSED_ATTRIBUTE, const size_t &thread_id UNUSED_ATTRIBUTE, const cid_t &begin_cid UNUSED_ATTRIBUTE, FileHandle *file_handles UNUSED_ATTRIBUTE) final {}


};

}
}
