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

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/logging/log_file.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger
//===--------------------------------------------------------------------===//

void LogFile::SetMaxCommitId(txn_id_t max_commit_id) {
  max_commit_id_ = max_commit_id;
}

txn_id_t LogFile::GetMaxCommitId() { return max_commit_id_; }

int LogFile::GetLogNumber() { return log_number_; }

std::string LogFile::GetLogFileName() { return log_file_name_; }

void LogFile::SetLogFileSize(int log_file_size) {
  log_file_size_ = log_file_size;
}

FILE *LogFile::GetFilePtr() { return log_file_; }

void LogFile::SetLogFileFD(int fd) { log_file_fd_ = fd; }

void LogFile::SetFilePtr(FILE *fp) { log_file_ = fp; }

}  // namespace logging
}  // namespace peloton
