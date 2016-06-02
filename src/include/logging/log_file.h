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
// LogFile metadata
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

  // set the maximum commit id in this file
  void SetMaxLogId(cid_t);

  // gethe maximum commit id for this file
  cid_t GetMaxLogId();

  // get the number of logs in this file
  int GetLogNumber();

  // get the name of this log file
  std::string GetLogFileName();

  // set the size of this log file
  void SetLogFileSize(int);

  // set the file descriptor for this file
  void SetLogFileFD(int);

  //set the file pointer
  void SetFilePtr(FILE *);

  //set the max delimeter this file contains
  void SetMaxDelimiter(cid_t);

  // get the max delimiter in this file
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
