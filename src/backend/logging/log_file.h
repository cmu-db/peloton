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
  LogFile(FileHandle file_handle, std::string log_file_name, int log_number,
          cid_t max_log_id_file, cid_t max_delimiter_file)
      : file_handle_(file_handle),
        log_file_name_(log_file_name),
        log_number_(log_number),
        max_log_id_file_(max_log_id_file),
        max_delimiter_file_(max_delimiter_file){};

  virtual ~LogFile(void){};

  void SetMaxLogId(cid_t);

  cid_t GetMaxLogId();

  int GetLogNumber();

  std::string GetLogFileName();

  void SetLogFileSize(int);

  void SetLogFileFD(int);

  void SetFilePtr(FILE *);

  void SetMaxDelimiter(cid_t);

  cid_t GetMaxDelimiter();

 private:
  FileHandle file_handle_;
  std::string log_file_name_;
  int log_number_;
  cid_t max_log_id_file_;
  cid_t max_delimiter_file_;

 protected:
};

}  // namespace logging
}  // namespace peloton
