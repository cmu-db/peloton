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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Timestamp checkpoint Manager
//===--------------------------------------------------------------------===//

class TimestampCheckpointManager : public CheckpointManager {
 public:
  TimestampCheckpointManager(const TimestampCheckpointManager &) = delete;
  TimestampCheckpointManager &operator=(const TimestampCheckpointManager &) = delete;
  TimestampCheckpointManager(TimestampCheckpointManager &&) = delete;
  TimestampCheckpointManager &operator=(TimestampCheckpointManager &&) = delete;

  TimestampCheckpointManager(const int thread_count) :
	  CheckpointManager(),
	  checkpointer_thread_count_(thread_count) {
  	SetCheckpointInterval(settings::SettingsManager::GetInt(settings::SettingId::checkpoint_interval));
  	SetCheckpointBaseDirectory(settings::SettingsManager::GetString(settings::SettingId::checkpoint_dir));
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
  void SetCheckpointInterval(const int interval) { checkpoint_interval_ = interval; }

  // setter for checkpoint base directory
  // checkpoint files are made in a below directory structure:
  //    base_directory / [epoch_id | checkpointing] / checkpoint_files
  void SetCheckpointBaseDirectory(const std::string &checkpoint_dir) {
	  // check the existence of checkpoint base directory.
	  // if not exist, then create the directory.
	  if (LoggingUtil::CheckDirectoryExistence(checkpoint_dir.c_str()) == false) {
		  LOG_INFO("Checkpoint base directory %s does not exist", checkpoint_dir.c_str());
		  if(LoggingUtil::CreateDirectory(checkpoint_dir.c_str(), 0700) == false) {
			  LOG_ERROR("Cannot create base directory for checkpoints: %s", checkpoint_dir.c_str());
		  }
	  }
	  checkpoint_base_dir_ = checkpoint_dir;
  }

  // get a latest checkpoint epoch for recovery
  eid_t GetRecoveryCheckpointEpoch();

 protected:

  //===--------------------------------------------------------------------===//
  // Checkpointing Functions
  //===--------------------------------------------------------------------===//

  // execute checkpointing in a designated interval
  void PerformCheckpointing();

  // checkpointing for the user tables
  void CreateUserTableCheckpoint(const cid_t begin_cid, concurrency::TransactionContext *txn);

  // checkpointing for the catalog tables
  void CreateCatalogTableCheckpoint(const cid_t begin_cid, concurrency::TransactionContext *txn);

  // create a checkpoint data file
  void CreateTableCheckpointFile(const storage::DataTable *table, const cid_t begin_cid, concurrency::TransactionContext *txn);

  // read table data and write it down to checkpoint data file
  void CheckpointingTableData(const storage::DataTable *target_table, const cid_t &begin_cid, FileHandle &file_handle);

  // read catalog objects for user tables and write it down to a checkpoint catalog file
  void CheckpointingCatalogObject(
  		const std::vector<std::shared_ptr<catalog::DatabaseCatalogObject>> &target_db_catalogs,
  		const std::vector<std::shared_ptr<catalog::TableCatalogObject>> &target_table_catalogs,
  		FileHandle &file_handle);

  // check the value is committed before the checkpointing begins
  bool IsVisible(const storage::TileGroupHeader *header, const oid_t &tuple_id, const cid_t &begin_cid);


  //===--------------------------------------------------------------------===//
  // Checkpoint Recovery Functions
  //===--------------------------------------------------------------------===//

  // recover user table checkpoints and these catalog objects
  bool LoadUserTableCheckpoint(const eid_t &epoch_id, concurrency::TransactionContext *txn);

  // recover catalog table checkpoints
  bool LoadCatalogTableCheckpoint(const eid_t &epoch_id, concurrency::TransactionContext *txn);

  // create a checkpoint data file
  bool LoadTableCheckpointFile(storage::DataTable *table, const eid_t &epoch_id, concurrency::TransactionContext *txn);

  // read a checkpoint catalog file and recover catalog objects for user tables
  bool RecoverCatalogObject(FileHandle &file_handle, concurrency::TransactionContext *txn);

  // read a checkpoint data file and recover the table
  void RecoverTableData(storage::DataTable *table, FileHandle &file_handle, concurrency::TransactionContext *txn);



  //===--------------------------------------------------------------------===//
  // Utility Functions for Checkpoint Directory
  //===--------------------------------------------------------------------===//

  void CreateCheckpointDirectory(const std::string &dir_name) {
  	std::string checkpoint_dir = checkpoint_base_dir_ + "/" + dir_name;
  	// check the existence of checkpoint directory.
  	// if exists, then remove all files in the directory
  	// else then create the directory.
  	if (LoggingUtil::CheckDirectoryExistence(checkpoint_dir.c_str()) == false) {
  		if (LoggingUtil::CreateDirectory(checkpoint_dir.c_str(), 0700) == false) {
  			LOG_ERROR("Cannot create directory in this checkpoints: %s", checkpoint_dir.c_str());
  		}
  	} else {
  		LOG_INFO("Checkpoint directory %s already exists, and delete all old files", checkpoint_dir.c_str());
  		if (LoggingUtil::RemoveDirectory(checkpoint_dir.c_str(), true) == false) {
  			LOG_ERROR("Cannot delete files in directory: %s", checkpoint_dir.c_str());
  		}
  	}
  }

  void CreateWorkingCheckpointDirectory() {
  	CreateCheckpointDirectory(checkpoint_working_dir_name_);
  }

  void MoveWorkingToCheckpointDirectory(const std::string dir_name) {
  	std::string working_dir_path = checkpoint_base_dir_ + "/" + checkpoint_working_dir_name_;
  	std::string checkpoint_dir_path = checkpoint_base_dir_ + "/" + dir_name;

  	CreateCheckpointDirectory(dir_name);
  	if (LoggingUtil::MoveFile(working_dir_path.c_str(), checkpoint_dir_path.c_str()) == false) {
  		LOG_ERROR("Cannot move checkpoint file from %s to %s", working_dir_path.c_str(), checkpoint_dir_path.c_str());
  	}
  }

  std::string GetCheckpointFileFullPath(const std::string &database_name, const std::string &table_name, const eid_t &epoch_id) {
  	return checkpoint_base_dir_ + "/" + std::to_string(epoch_id) + "/" + checkpoint_filename_prefix_ + "_" + database_name + "_" + table_name;
  }
  std::string GetWorkingCheckpointFileFullPath(const std::string &database_name, const std::string &table_name) {
  	return checkpoint_base_dir_ + "/" + checkpoint_working_dir_name_ + "/" + checkpoint_filename_prefix_ + "_" + database_name + "_" + table_name;
  }
  std::string GetCatalogFileFullPath(const eid_t &epoch_id) {
  	return checkpoint_base_dir_ + "/" + std::to_string(epoch_id) + "/" + catalog_filename_prefix_;
  }
  std::string GetWorkingCatalogFileFullPath() {
  	return checkpoint_base_dir_ + "/" + checkpoint_working_dir_name_ + "/" + catalog_filename_prefix_;
  }

  void RemoveOldCheckpoints(const eid_t &begin_epoch_id) {
  	std::vector<std::string> dir_name_list;

  	// Get list of checkpoint directories
  	if (LoggingUtil::GetDirectoryList(checkpoint_base_dir_.c_str(), dir_name_list) == false) {
  		LOG_ERROR("Failed to get directory list in %s", checkpoint_base_dir_.c_str());
  	}

  	// Remove old checkpoint directory
  	for (auto dir_name = dir_name_list.begin(); dir_name != dir_name_list.end(); dir_name++) {
  		if (strcmp((*dir_name).c_str(), checkpoint_working_dir_name_.c_str()) != 0) {
  			eid_t epoch_id = std::strtoul((*dir_name).c_str(), NULL, 10);
  			if (epoch_id == 0) {
  				LOG_ERROR("Unexpected epoch value in checkpoints directory: %s", (*dir_name).c_str());
  			} else if (epoch_id == begin_epoch_id) {
  				// not be old
  				continue;
  			}
  		}

  		std::string remove_dir = checkpoint_base_dir_ + '/' + (*dir_name);
  		bool ret = LoggingUtil::RemoveDirectory(remove_dir.c_str(), false);
  		if (ret == false) {
  			LOG_ERROR("Failure to remove checkpoint dir: %s", remove_dir.c_str());
  		}
  	}
  }

 private:
  int checkpointer_thread_count_;
  std::unique_ptr<std::thread> central_checkpoint_thread_;

  int checkpoint_interval_ = 30;

  std::string checkpoint_base_dir_ = "./checkpoints";
  std::string checkpoint_working_dir_name_ = "checkpointing";
  std::string checkpoint_filename_prefix_ = "checkpoint";
  std::string catalog_filename_prefix_ = "checkpoint_catalog";


};

}  // namespace logging
}  // namespace peloton
