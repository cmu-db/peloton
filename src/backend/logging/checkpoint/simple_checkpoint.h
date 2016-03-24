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
  SimpleCheckpoint() {}

  static SimpleCheckpoint &GetInstance();

  // Inherited functions
  void Init();
  void DoCheckpoint();
  void DoRecovery();

  // Internal functions
  void CreateCheckpointFile();
  void Persist();

 private:
  std::vector<LogRecord *> records_;

  FILE *checkpoint_file_ = nullptr;

  int checkpoint_file_fd_ = INVALID_FILE_DESCRIPTOR;

  // Default checkpoint interval is 20 seconds
  int64_t checkpoint_interval_ = 20;
};

}  // namespace logging
}  // namespace peloton
