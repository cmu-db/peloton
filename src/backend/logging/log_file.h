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
          int log_number, cid_t max_log_id_file, cid_t max_delimiter_file)
      : log_file_(log_file),
        log_file_name_(log_file_name),
        log_file_fd_(log_file_fd),
        log_number_(log_number),
        max_log_id_file_(max_log_id_file),
	max_delimiter_file_(max_delimiter_file) {
    log_file_size_ = 0;
  };

  virtual ~LogFile(void){};

  void SetMaxLogId(cid_t);

  cid_t GetMaxLogId();

  int GetLogNumber();

  std::string GetLogFileName();

  void SetLogFileSize(int);

  FILE *GetFilePtr();

  void SetLogFileFD(int);

  void SetFilePtr(FILE *);

  void SetMaxDelimiter(cid_t);

  cid_t GetMaxDelimiter();

 private:
  FILE *log_file_;
  std::string log_file_name_;
  int log_file_fd_;
  int log_file_size_;
  int log_number_;
  cid_t max_log_id_file_;
  cid_t max_delimiter_file_;

 protected:
};

}  // namespace logging
}  // namespace peloton
