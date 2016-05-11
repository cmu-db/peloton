/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger
//===--------------------------------------------------------------------===//

class LogFile {
 public:
  LogFile(FILE *log_file, std::string log_file_name, int log_file_fd,
          int log_number, txn_id_t max_commit_id)
      : log_file_(log_file),
        log_file_name_(log_file_name),
        log_file_fd_(log_file_fd),
        log_number_(log_number),
        max_commit_id_(max_commit_id) {
    log_file_size_ = 0;
  };

  virtual ~LogFile(void){};

  void SetMaxCommitId(txn_id_t);

  txn_id_t GetMaxCommitId();

  int GetLogNumber();

  std::string GetLogFileName();

  void SetLogFileSize(int);

  FILE *GetFilePtr();

  void SetLogFileFD(int);

  void SetFilePtr(FILE *);

 private:
  FILE *log_file_;
  std::string log_file_name_;
  int log_file_fd_;
  int log_file_size_;
  int log_number_;
  txn_id_t max_commit_id_;

 protected:
};

}  // namespace logging
}  // namespace peloton
