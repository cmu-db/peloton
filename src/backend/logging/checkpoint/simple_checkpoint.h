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
  SimpleCheckpoint() : Checkpoint() {}

  static SimpleCheckpoint &GetInstance();

  // Inherited functions
  void Init();
  void DoCheckpoint();
  bool DoRecovery();

  // Internal functions
  void InsertTuple();

  bool Execute(executor::AbstractExecutor *scan_executor,
               concurrency::Transaction *txn, storage::DataTable *target_table,
               oid_t database_oid);
  void CreateCheckpointFile();

  void Persist();

  void SetLogger(BackendLogger *logger);

 private:
  std::vector<LogRecord *> records_;

  FILE *checkpoint_file_ = nullptr;

  int checkpoint_file_fd_ = INVALID_FILE_DESCRIPTOR;

  // Size of the checkpoint file
  size_t checkpoint_file_size_ = 0;

  // Default checkpoint interval
  int64_t checkpoint_interval_ = 10;

  BackendLogger *logger_ = nullptr;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid_ = 0;
};

}  // namespace logging
}  // namespace peloton
