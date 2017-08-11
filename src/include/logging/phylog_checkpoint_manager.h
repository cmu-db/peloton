//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/phylog_checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/checkpoint_manager.h"

namespace peloton {
namespace logging {

/**
 * checkpoint file name layout :
 * 
 * dir_name + "/" + prefix + "_" + checkpointer_id + "_" + database_id + "_" + table_id + "_" + epoch_id
 *
 *
 * checkpoint file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | tuple_1 | tuple_2 | tuple_3 | ...
 *  -----------------------------------------------------------------------------
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class PhyLogCheckpointManager : public CheckpointManager {
  // Deleted functions
  PhyLogCheckpointManager(const PhyLogCheckpointManager &) = delete;
  PhyLogCheckpointManager &operator=(const PhyLogCheckpointManager &) = delete;
  PhyLogCheckpointManager(PhyLogCheckpointManager &&) = delete;
  PhyLogCheckpointManager &operator=(const PhyLogCheckpointManager &&) = delete;


public:
  PhyLogCheckpointManager() {}
  virtual ~PhyLogCheckpointManager() {}

  static PhyLogCheckpointManager& GetInstance() {
    static PhyLogCheckpointManager checkpoint_manager;
    return checkpoint_manager;
  }

private:

  virtual void RecoverTable(storage::DataTable *, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) final;

  virtual void CheckpointTable(storage::DataTable *, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) final;

};

}
}
