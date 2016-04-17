/*-------------------------------------------------------------------------
 *
 * simple_checkpoint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint/simple_checkpoint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/checkpoint.h"
#include "backend/logging/log_record.h"
#include "backend/executor/seq_scan_executor.h"

#include <thread>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

class SimpleCheckpoint : public Checkpoint {
 public:
  SimpleCheckpoint(const SimpleCheckpoint &) = delete;
  SimpleCheckpoint &operator=(const SimpleCheckpoint &) = delete;
  SimpleCheckpoint(SimpleCheckpoint &&) = delete;
  SimpleCheckpoint &operator=(SimpleCheckpoint &&) = delete;
  SimpleCheckpoint();
  ~SimpleCheckpoint();

  // Inherited functions
  void DoCheckpoint();

  cid_t DoRecovery();

  // Internal functions
  void InsertTuple(cid_t commit_id);

  void Scan(storage::DataTable *target_table, cid_t start_cid,
            oid_t database_oid);

  // Getters and Setters
  void SetLogger(BackendLogger *logger);

  std::vector<std::shared_ptr<LogRecord>> GetRecords();

 private:
  void CreateFile();

  void Persist();

  void Cleanup();

  void InitVersionNumber();

  std::vector<std::shared_ptr<LogRecord>> records_;

  FileHandle file_handle_ = INVALID_FILE_HANDLE;

  BackendLogger *logger_ = nullptr;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid_ = 0;

  // commit id of current checkpoint
  cid_t start_commit_id = 0;
};

}  // namespace logging
}  // namespace peloton
