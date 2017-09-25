//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <thread>

#include "type/abstract_pool.h"
#include "type/types.h"
#include "common/logger.h"
#include "logging/logging_util.h"


//This is only for the mac build!!!
#define DEFAULT_CHECKPOINT_INTERVAL 30


namespace peloton {

namespace storage {
  class Database;
  class DataTable;
  class TileGroup;
  class TileGroupHeader;
}

namespace logging {

class CheckpointManager {
  // Deleted functions
  CheckpointManager(const CheckpointManager &) = delete;
  CheckpointManager &operator=(const CheckpointManager &) = delete;
  CheckpointManager(CheckpointManager &&) = delete;
  CheckpointManager &operator=(const CheckpointManager &&) = delete;


public:
  CheckpointManager() :
    is_running_(false),
    checkpoint_interval_(DEFAULT_CHECKPOINT_INTERVAL) {
    recovery_thread_count_ = 1;
    // max_checkpointer_count_ = std::thread::hardware_concurrency() / 2;
    max_checkpointer_count_ = 10;
  }
  virtual ~CheckpointManager() {}

  void SetCheckpointInterval(const int &checkpoint_interval) {
    checkpoint_interval_ = checkpoint_interval;
  }

  void SetDirectories(const std::vector<std::string> &checkpoint_dirs) {
    if (checkpoint_dirs.size() > 0) {
      ckpt_pepoch_dir_ = checkpoint_dirs.at(0);
    }
    // check the existence of checkpoint directories.
    // if not exists, then create the directory.
    for (auto checkpoint_dir : checkpoint_dirs) {
      if (LoggingUtil::CheckDirectoryExistence(checkpoint_dir.c_str()) == false) {
        LOG_INFO("Checkpoint directory %s is not accessible or does not exist", checkpoint_dir.c_str());
        bool res = LoggingUtil::CreateDirectory(checkpoint_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", checkpoint_dir.c_str());
        }
      }
    }

    checkpoint_dirs_ = checkpoint_dirs;
    checkpointer_count_ = checkpoint_dirs_.size();
  }

  void SetRecoveryThreadCount(const size_t &recovery_thread_count) {
    recovery_thread_count_ = recovery_thread_count;
    if (recovery_thread_count_ > max_checkpointer_count_) {
      LOG_ERROR("# recovery thread cannot be larger than # max checkpointer");
      exit(EXIT_FAILURE);
    }
  }

  virtual void StartCheckpointing();
  virtual void StopCheckpointing();

  size_t DoRecovery();

protected:
  std::string GetCheckpointFileFullPath(size_t checkpointer_id, size_t virtual_checkpointer_id, oid_t database_idx, oid_t table_idx, size_t epoch_id) {
    return checkpoint_dirs_.at(checkpointer_id) + "/" + checkpoint_filename_prefix_ + "_" + std::to_string(virtual_checkpointer_id) + "_" + std::to_string(database_idx) + "_" + std::to_string(table_idx) + "_" + std::to_string(epoch_id);
  }

  // recover logic
  size_t RecoverPepoch(std::vector<std::vector<size_t>> &database_structures);
  void RecoverCheckpoint(const size_t &epoch_id, const std::vector<std::vector<size_t>> &database_structures);
  void RecoverCheckpointThread(const size_t &thread_id, const size_t &epoch_id, const std::vector<std::vector<size_t>> &database_structures, FileHandle ***file_handles);

  virtual void RecoverTable(storage::DataTable *, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) = 0;

  virtual void PrepareTables(const std::vector<std::vector<size_t>> &database_structures UNUSED_ATTRIBUTE) {}

  // checkpointing logic

  void Running();
  void PerformCheckpoint(const cid_t &begin_cid, const std::vector<std::vector<size_t>> &database_structures);
  void PerformCheckpointThread(const size_t &thread_id, const cid_t &begin_cid, const std::vector<std::vector<size_t>> &database_structures, FileHandle ***file_handles);

  virtual void CheckpointTable(storage::DataTable *, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) = 0;

  bool IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &begin_cid);


protected:

  size_t recovery_thread_count_;

  size_t max_checkpointer_count_;

  bool is_running_;

  int checkpoint_interval_;

//  const int DEFAULT_CHECKPOINT_INTERVAL = 30;

  size_t checkpointer_count_;
  std::vector<std::string> checkpoint_dirs_;

  const std::string checkpoint_filename_prefix_ = "checkpoint";

  std::unique_ptr<std::thread> central_checkpoint_thread_;

  std::string ckpt_pepoch_dir_ = "/home/paulo/log";

  const std::string ckpt_pepoch_filename_ = "checkpoint_pepoch";

  CopySerializeOutput ckpt_pepoch_buffer_;

  std::vector<std::unique_ptr<type::AbstractPool>> recovery_pools_;
};

}
}
