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
 private:
 public:
  LogFile(FILE *log_file, std::string log_file_name, int log_file_fd,
          int log_number)
      : log_file_(log_file),
        log_file_name_(log_file_name),
        log_file_fd_(log_file_fd),
        log_number_(log_number) {
    log_file_size_ = 0;
  };

  virtual ~LogFile(void){};

  FILE *log_file_;
  std::string log_file_name_;
  int max_commit_id_;  // we may not need this later..
  int log_file_fd_;
  int log_file_size_;
  int log_number_;

 protected:
};

}  // namespace logging
}  // namespace peloton
