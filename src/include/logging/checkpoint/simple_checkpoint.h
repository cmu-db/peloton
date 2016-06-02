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

#include <memory>
#include <thread>

#include "backend/logging/checkpoint.h"

namespace peloton {
namespace logging {

class LogRecord;
class BackendLogger;

//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

class SimpleCheckpoint : public Checkpoint {
 public:
  SimpleCheckpoint(const SimpleCheckpoint &) = delete;
  SimpleCheckpoint &operator=(const SimpleCheckpoint &) = delete;
  SimpleCheckpoint(SimpleCheckpoint &&) = delete;
  SimpleCheckpoint &operator=(SimpleCheckpoint &&) = delete;
  SimpleCheckpoint(bool test_mode);
  ~SimpleCheckpoint();

  // Inherited functions
  void DoCheckpoint();

  cid_t DoRecovery();

  // Internal functions
  void InsertTuple(cid_t commit_id);

  void Scan(storage::DataTable *target_table, oid_t database_oid);

  // Getters and Setters
  void SetLogger(BackendLogger *logger);

  std::vector<std::shared_ptr<LogRecord>> GetRecords();

  inline void SetStartCommitId(cid_t start_commit_id) {
    start_commit_id_ = start_commit_id;
  }

 private:
  void CreateFile();

  void Persist();

  void Cleanup();

  void InitVersionNumber();

  std::vector<std::shared_ptr<LogRecord>> records_;

  FileHandle file_handle_ = INVALID_FILE_HANDLE;

  std::unique_ptr<BackendLogger> logger_;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid_ = 0;

  // commit id of current checkpoint
  cid_t start_commit_id_ = 0;
};

}  // namespace logging
}  // namespace peloton
