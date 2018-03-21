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
	  checkpointer_thread_count_(thread_count),
	  checkpoint_interval_(DEFAULT_CHECKPOINT_INTERVAL){
  	SetCheckpointBaseDirectory(DEFAULT_CHECKPOINT_BASE_DIR);
  }

  virtual ~TimestampCheckpointManager() {}

  static TimestampCheckpointManager &GetInstance(const int thread_count = 1) {
    static TimestampCheckpointManager checkpoint_manager(thread_count);
    return checkpoint_manager;
  }

  virtual void Reset() { is_running_ = false; }

  virtual void StartCheckpointing();

  virtual void StopCheckpointing();

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

  void SetCheckpointInterval(const int interval) { checkpoint_interval_ = interval; }

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

  bool DoRecovery();


 protected:
  void Running();
  std::vector<std::vector<size_t>>& CreateDatabaseStructures();
  void PerformCheckpointing();
  void CheckpointingTable(const storage::DataTable *target_table, const cid_t &begin_cid, FileHandle &file_handle);
  void CheckpointingCatalog(
  		const std::vector<std::shared_ptr<catalog::DatabaseCatalogObject>> &target_db_catalogs,
  		const std::vector<std::shared_ptr<catalog::TableCatalogObject>> &target_table_catalogs,
  		FileHandle &file_handle);
  bool IsVisible(const storage::TileGroupHeader *header, const oid_t &tuple_id, const cid_t &begin_cid);
  bool PerformCheckpointRecovery(const eid_t &epoch_id);
  void RecoverCatalog(FileHandle &file_handle, concurrency::TransactionContext *txn);
  void RecoverTable(storage::DataTable *table, FileHandle &file_handle, concurrency::TransactionContext *txn);

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

  eid_t GetRecoveryCheckpointEpoch() {
  	eid_t max_epoch = INVALID_EID;
  	std::vector<std::string> dir_name_list;

  	// Get list of checkpoint directories
  	if (LoggingUtil::GetDirectoryList(checkpoint_base_dir_.c_str(), dir_name_list) == false) {
  		LOG_ERROR("Failed to get directory list %s", checkpoint_base_dir_.c_str());
  	}

  	// Get the newest epoch from checkpoint directories
  	for (auto dir_name = dir_name_list.begin(); dir_name != dir_name_list.end(); dir_name++) {
  		eid_t epoch_id;

  		if (strcmp((*dir_name).c_str(), checkpoint_working_dir_name_.c_str()) == 0) {
  			continue;
  		}
  		epoch_id = std::strtoul((*dir_name).c_str(), NULL, 10);

  		if (epoch_id == INVALID_EID) {
  			LOG_ERROR("Unexpected epoch value in checkpoints directory: %s", (*dir_name).c_str());
  		}
  		max_epoch = (epoch_id > max_epoch)? epoch_id : max_epoch;
  	}
  	LOG_DEBUG("max epoch : %lu", max_epoch);
  	return max_epoch;
  }

 private:
  int checkpointer_thread_count_;
  std::unique_ptr<std::thread> central_checkpoint_thread_;

  const int DEFAULT_CHECKPOINT_INTERVAL = 30;
  int checkpoint_interval_;

  const std::string DEFAULT_CHECKPOINT_BASE_DIR = "./checkpoints";
  std::string checkpoint_base_dir_;
  std::string checkpoint_working_dir_name_ = "checkpointing";
  std::string checkpoint_filename_prefix_ = "checkpoint";
  std::string catalog_filename_prefix_ = "checkpoint_catalog";


};

}  // namespace logging
}  // namespace peloton
