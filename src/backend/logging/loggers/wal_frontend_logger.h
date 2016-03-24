/*-------------------------------------------------------------------------
 *
 * wal_frontend_logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wal_frontend_logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontend_logger.h"
#include "backend/logging/log_file.h"
#include <dirent.h>

namespace peloton {

class VarlenPool;

namespace concurrency {
class Transaction;
}

namespace logging {

//===--------------------------------------------------------------------===//
// Write Ahead Frontend Logger
//===--------------------------------------------------------------------===//

class WriteAheadFrontendLogger : public FrontendLogger {
 public:
  WriteAheadFrontendLogger(void);

  ~WriteAheadFrontendLogger(void);

  void FlushLogRecords(void);

  //===--------------------------------------------------------------------===//
  // Recovery
  //===--------------------------------------------------------------------===//

  void DoRecovery(void);

  void AddTransactionToRecoveryTable(void);

  void RemoveTransactionFromRecoveryTable(void);

  void MoveCommittedTuplesToRecoveryTxn(concurrency::Transaction *recovery_txn);

  void AbortTuplesFromRecoveryTable(void);

  void MoveTuples(concurrency::Transaction *destination,
                  concurrency::Transaction *source);

  void InsertTuple(concurrency::Transaction *recovery_txn);

  void DeleteTuple(concurrency::Transaction *recovery_txn);

  void UpdateTuple(concurrency::Transaction *recovery_txn);

  void AbortActiveTransactions();

  void AbortTuples(concurrency::Transaction *txn);

  void InitLogFilesList() {
    // TODO: handle case where this may be just a database restart instead of
    // fresh start
    DIR *dirp;
    struct dirent *file;
    std::string base_name = "peloton_log_";

    LOG_INFO("Trying to read log directory");

    dirp = opendir(".");
    if (dirp == NULL) LOG_INFO("Opendir failed: %s", strerror(errno));
    // TODO use this data to populate in memory data structures
    while ((file = readdir(dirp)) != NULL) {
      if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
        // found a log file!
        LOG_INFO("Found a log file with name %s", file->d_name);
      }
    }

    this->log_file_counter_ = 0;
    this->CreateNewLogFile(false);
  }

  void CreateNewLogFile(bool close_old_file) {
    int new_file_num = log_file_counter_;
    LOG_INFO("new_file_num is %d", new_file_num);
    std::string new_file_name;

    new_file_name = "peloton_log_" + std::to_string(new_file_num) + ".log";

    FILE *log_file = fopen(new_file_name.c_str(), "wb");
    if (log_file == NULL) {
      LOG_ERROR("log_file is NULL");
    }

    this->log_file = log_file;

    int log_file_fd = fileno(log_file);

    if (log_file_fd == -1) {
      LOG_ERROR("log_file_fd is -1");
    }

    this->log_file_fd = log_file_fd;
    LOG_INFO("log_file_fd of newly created file is %d", this->log_file_fd);

    LogFile *new_log_file_object =
        new LogFile(log_file, new_file_name, log_file_fd);

    if (close_old_file) {  // must close last opened file
      int file_list_size = this->log_files_.size();

      if (file_list_size != 0) {
        this->log_files_[file_list_size - 1]->log_file_size_ =
            this->log_file_size;
        fclose(this->log_files_[file_list_size - 1]->log_file_);
        this->log_files_[file_list_size - 1]->log_file_fd_ = -1;  // invalidate
        // TODO set max commit_id here!
      }
    }

    this->log_files_.push_back(new_log_file_object);

    log_file_counter_++;  // finally, increment log_file_counter_
    LOG_INFO("log_file_counter is %d", log_file_counter_);
  }

  bool FileSwitchCondIsTrue(__attribute__((unused)) int fsync_count) {
    // TODO have a good heuristic here
    // return false;

    return (fsync_count % 10000) == 0;
  }

 private:
  std::string GetLogFileName(void);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // File pointer and descriptor
  FILE *log_file;
  int log_file_fd;

  // Size of the log file
  size_t log_file_size;

  // Txn table during recovery
  std::map<txn_id_t, concurrency::Transaction *> recovery_txn_table;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid = 0;

  // pool for allocating non-inlined values
  VarlenPool *recovery_pool;

  // abj1 adding code here!
  std::vector<LogFile *> log_files_;

  int log_file_counter_;
};

}  // namespace logging
}  // namespace peloton
