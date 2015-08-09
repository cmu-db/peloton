/*-------------------------------------------------------------------------
 *
 * stdoutbackendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutbackendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/stdoutbackendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief Recording log record
 * @param log record 
 */
void StdoutBackendLogger::log(LogRecord record){
  stdout_buffer.push_back(record);
}

/**
 * @brief set the local commit offset to size  of buffer
 */
void StdoutBackendLogger::localCommit(){
  local_commit_offset = stdout_buffer.size();
}

}  // namespace logging
}  // namespace peloton
