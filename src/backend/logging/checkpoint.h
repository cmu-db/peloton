/*-------------------------------------------------------------------------
 *
 * checkpoint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cassert>
#include <string>
#include <sys/stat.h>

#include "backend/logging/log_manager.h"
#include "backend/logging/checkpoint_manager.h"
#include "backend/logging/backend_logger.h"
#include "backend/common/pool.h"
#include "backend/storage/tile.h"
#include "backend/storage/database.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
class Checkpoint {
  friend class CheckpointManager;

 public:
  Checkpoint(bool disable_file_access)
      : disable_file_access(disable_file_access) {
    pool.reset(new VarlenPool(BACKEND_TYPE_MM));
  }

  virtual ~Checkpoint(void) { pool.reset(); }

  // Main body of the checkpoint thread
  void MainLoop(void);

  // Do checkpoint periodically
  virtual void DoCheckpoint() = 0;

  // Do recovery from most recent version of checkpoint
  virtual cid_t DoRecovery() = 0;

  void RecoverTuple(storage::Tuple *tuple, storage::DataTable *table,
                    ItemPointer target_location, cid_t commit_id);

  inline cid_t GetMostRecentCheckpointCid() {
    return most_recent_checkpoint_cid;
  }

  inline CheckpointStatus GetCheckpointStatus() { return checkpoint_status; }

 protected:
  std::string ConcatFileName(std::string checkpoint_dir, int version);

  void InitDirectory();

  // whether file access is disabled. mainly used for testing
  bool disable_file_access = false;

  // Default checkpoint interval (seconds)
  //TODO set interval to configurable variable
  int64_t checkpoint_interval_ = 5;

  // variable length memory pool
  //TODO better periodically clean up varlen pool?
  std::unique_ptr<VarlenPool> pool;

  //TODO set directory to configurable variables
  std::string checkpoint_dir = "pl_checkpoint";

  // the version of next checkpoint
  int checkpoint_version = -1;

  // prefix for checkpoint file name
  const std::string FILE_PREFIX = "peloton_checkpoint_";

  // suffix for checkpoint file name
  const std::string FILE_SUFFIX = ".log";

  // current status
  CheckpointStatus checkpoint_status = CHECKPOINT_STATUS_INVALID;

  // the most recent successful checkpoint cid
  cid_t most_recent_checkpoint_cid = INVALID_CID;

 private:
  // Get a checkpointer
  static std::unique_ptr<Checkpoint> GetCheckpoint(
      CheckpointType checkpoint_type, bool test_mode);
};

}  // namespace logging
}  // namespace peloton
