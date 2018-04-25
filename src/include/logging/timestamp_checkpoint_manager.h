//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_checkpoint_manager.h
//
// Identification: src/include/logging/timestamp_checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/checkpoint_manager.h"

#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/schema.h"
#include "logging/logging_util.h"
#include "settings/settings_manager.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "util/string_util.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Timestamp checkpoint Manager
//===--------------------------------------------------------------------===//

class TimestampCheckpointManager : public CheckpointManager {
 public:
  TimestampCheckpointManager(const TimestampCheckpointManager &) = delete;
  TimestampCheckpointManager &operator=(const TimestampCheckpointManager &) =
      delete;
  TimestampCheckpointManager(TimestampCheckpointManager &&) = delete;
  TimestampCheckpointManager &operator=(TimestampCheckpointManager &&) = delete;

  TimestampCheckpointManager(const int thread_count)
      : CheckpointManager(), checkpointer_thread_count_(thread_count) {
    SetCheckpointInterval(settings::SettingsManager::GetInt(
        settings::SettingId::checkpoint_interval));
    SetCheckpointBaseDirectory(settings::SettingsManager::GetString(
        settings::SettingId::checkpoint_dir));
  }

  virtual ~TimestampCheckpointManager() {}

  static TimestampCheckpointManager &GetInstance(const int thread_count = 1) {
    static TimestampCheckpointManager checkpoint_manager(thread_count);
    return checkpoint_manager;
  }

  virtual void Reset() { is_running_ = false; }

  virtual void StartCheckpointing();

  virtual void StopCheckpointing();

  virtual bool DoCheckpointRecovery();

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

  // setter for checkpoint interval
  void SetCheckpointInterval(const int interval) {
    checkpoint_interval_ = interval;
  }

  // setter for checkpoint base directory
  // checkpoint files are made in a below directory structure:
  //    base_directory / [epoch_id | checkpointing] / checkpoint_files
  void SetCheckpointBaseDirectory(const std::string &dir_name) {
    // check the existence of checkpoint base directory.
    // if not exist, then create the directory.
    if (LoggingUtil::CheckDirectoryExistence(dir_name.c_str()) == false) {
      LOG_INFO("Create base checkpoint directory %s", dir_name.c_str());
      CreateDirectory(dir_name);
    }
    checkpoint_base_dir_ = dir_name;
  }

  // get a recovered epoch id, or a latest checkpoint epoch for recovery
  eid_t GetRecoveryCheckpointEpoch();

 protected:
  //===--------------------------------------------------------------------===//
  // Checkpointing Functions
  //===--------------------------------------------------------------------===//

  // execute checkpointing in a designated interval
  void PerformCheckpointing();

  // checkpointing for the user tables
  void CreateUserTableCheckpoint(const cid_t begin_cid,
                                 concurrency::TransactionContext *txn);

  // checkpointing for the catalog tables
  void CreateCatalogTableCheckpoint(const cid_t begin_cid,
                                    concurrency::TransactionContext *txn);

  // read table data and write it down to checkpoint data file
  void CheckpointingTableData(const storage::DataTable *table,
                              const cid_t &begin_cid, FileHandle &file_handle);

  // read table data without tile group and write it down to checkpoint data
  // file
  // for catalog table checkpointing
  void CheckpointingTableDataWithoutTileGroup(const storage::DataTable *table,
                                              const cid_t &begin_cid,
                                              FileHandle &file_handle);

  // check the value is committed before the checkpointing begins
  bool IsVisible(const storage::TileGroupHeader *header, const oid_t &tuple_id,
                 const cid_t &begin_cid);

  // read storage objects data for user tables and write it down to a checkpoint
  // metadata file
  void CheckpointingStorageObject(FileHandle &file_handle,
                                  concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Checkpoint Recovery Functions
  //===--------------------------------------------------------------------===//

  // recover catalog table checkpoints
  bool LoadCatalogTableCheckpoint(const eid_t &epoch_id,
                                  concurrency::TransactionContext *txn);

  // recover user table checkpoints and these catalog objects
  bool LoadUserTableCheckpoint(const eid_t &epoch_id,
                               concurrency::TransactionContext *txn);

  // read a checkpoint catalog file and recover catalog objects for user tables
  bool RecoverStorageObject(FileHandle &file_handle,
                            concurrency::TransactionContext *txn);

  // read a checkpoint data file and recover the table
  // this function is provided for checkpointed user table
  void RecoverTableData(storage::DataTable *table, FileHandle &file_handle,
                        concurrency::TransactionContext *txn);

  // read a checkpoint data file without tile group and recover the table
  // this function is provided for initialized catalog table not including
  // default value
  // return: inserted tuple count into table
  oid_t RecoverTableDataWithoutTileGroup(storage::DataTable *table,
                                         FileHandle &file_handle,
                                         concurrency::TransactionContext *txn);

  // read a checkpoint data file with duplicate check without tile group
  // this function keeps default values of catalogs
  // return: inserted tuple count into table without default value
  oid_t RecoverTableDataWithDuplicateCheck(
      storage::DataTable *table, FileHandle &file_handle,
      concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Utility Functions for Checkpoint Directory
  //===--------------------------------------------------------------------===//

  // create hierarchical directory
  void CreateDirectory(const std::string &dir_name) {
    std::string sub_dir_path = "";
    for (auto sub_dir_name : StringUtil::Split(dir_name, '/')) {
      sub_dir_path += sub_dir_name + "/";
      if (LoggingUtil::CheckDirectoryExistence(sub_dir_path.c_str()) == false) {
        LOG_TRACE("Create sub directory %s", sub_dir_path.c_str());
        if (LoggingUtil::CreateDirectory(sub_dir_path.c_str(), 0700) == false) {
          LOG_ERROR("Cannot create directory in this checkpoints: %s",
                    dir_name.c_str());
          break;
        }
      }
    }
  }

  // create checkpoint directory in base directory (default: ./data/checkpoint/)
  void CreateCheckpointDirectory(const std::string &dir_name) {
    std::string checkpoint_dir = checkpoint_base_dir_ + "/" + dir_name;

    // check the existence of checkpoint directory.
    // if exists, then remove all files in the directory
    // else then create the directory.
    if (LoggingUtil::CheckDirectoryExistence(checkpoint_dir.c_str()) == false) {
      LOG_TRACE("Create checkpoint directory %s", checkpoint_dir.c_str());
      CreateDirectory(checkpoint_dir);
    } else {
      LOG_TRACE("Found checkpoint directory %s, and delete all old files",
                checkpoint_dir.c_str());
      if (LoggingUtil::RemoveDirectory(checkpoint_dir.c_str(), true) == false) {
        LOG_ERROR("Cannot delete files in directory: %s",
                  checkpoint_dir.c_str());
      }
    }
  }

  // create working checkpoint directory in base directory.
  // (default: ./data//checkpoint/checkpointing)
  // this avoids to recover the failed (broken) checkpointing files.
  // e.g. system crash during checkpointing
  void CreateWorkingCheckpointDirectory() {
    CreateCheckpointDirectory(checkpoint_working_dir_name_);
  }

  // move the working checkpoint directory to completed checkpoint directory
  // named epoch id that the checkpointing is started.
  void MoveWorkingToCheckpointDirectory(const std::string dir_name) {
    std::string working_dir_path =
        checkpoint_base_dir_ + "/" + checkpoint_working_dir_name_;
    std::string checkpoint_dir_path = checkpoint_base_dir_ + "/" + dir_name;

    CreateCheckpointDirectory(dir_name);
    if (LoggingUtil::MoveFile(working_dir_path.c_str(),
                              checkpoint_dir_path.c_str()) == false) {
      LOG_ERROR("Cannot move checkpoint file from %s to %s",
                working_dir_path.c_str(), checkpoint_dir_path.c_str());
    }
  }

  std::string GetCheckpointFileFullPath(const std::string &database_name,
                                        const std::string &table_name,
                                        const eid_t &epoch_id) {
    return checkpoint_base_dir_ + "/" + std::to_string(epoch_id) + "/" +
           checkpoint_filename_prefix_ + "_" + database_name + "_" + table_name;
  }
  std::string GetWorkingCheckpointFileFullPath(const std::string &database_name,
                                               const std::string &table_name) {
    return checkpoint_base_dir_ + "/" + checkpoint_working_dir_name_ + "/" +
           checkpoint_filename_prefix_ + "_" + database_name + "_" + table_name;
  }
  std::string GetMetadataFileFullPath(const eid_t &epoch_id) {
    return checkpoint_base_dir_ + "/" + std::to_string(epoch_id) + "/" +
           metadata_filename_prefix_;
  }
  std::string GetWorkingMetadataFileFullPath() {
    return checkpoint_base_dir_ + "/" + checkpoint_working_dir_name_ + "/" +
           metadata_filename_prefix_;
  }

  // remove old checkpoints except for the designated epoch id (latest
  // checkpointing time)
  // TODO: Consider whether some old checkpoints will remain
  void RemoveOldCheckpoints(const eid_t &begin_epoch_id) {
    std::vector<std::string> dir_name_list;

    // Get list of checkpoint directories
    if (LoggingUtil::GetDirectoryList(checkpoint_base_dir_.c_str(),
                                      dir_name_list) == false) {
      LOG_ERROR("Failed to get directory list in %s",
                checkpoint_base_dir_.c_str());
    }

    // Remove old checkpoint directory
    for (auto dir_name : dir_name_list) {
      if (strcmp(dir_name.c_str(), checkpoint_working_dir_name_.c_str()) != 0) {
        eid_t epoch_id = std::strtoul(dir_name.c_str(), NULL, 10);
        if (epoch_id == 0) {
          LOG_ERROR("Unexpected epoch value in checkpoints directory: %s",
                    dir_name.c_str());
        } else if (epoch_id == begin_epoch_id) {
          // not be old
          continue;
        }
      }

      std::string remove_dir = checkpoint_base_dir_ + '/' + dir_name;
      if (LoggingUtil::RemoveDirectory(remove_dir.c_str(), false) == false) {
        LOG_ERROR("Failure to remove checkpoint dir: %s", remove_dir.c_str());
      }
    }
  }

 private:
  int checkpointer_thread_count_;
  std::unique_ptr<std::thread> central_checkpoint_thread_;

  int checkpoint_interval_ = 30;

  std::string checkpoint_base_dir_;
  std::string checkpoint_working_dir_name_ = "checkpointing";
  std::string checkpoint_filename_prefix_ = "checkpoint";
  std::string metadata_filename_prefix_ = "checkpoint_metadata";

  eid_t recovered_epoch_id = INVALID_EID;
};

}  // namespace logging
}  // namespace peloton
