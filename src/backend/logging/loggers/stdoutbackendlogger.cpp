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
void StdoutBackendLogger::Log(LogRecord record){
  {
    std::lock_guard<std::mutex> lock(stdout_buffer_mutex);
    stdout_buffer.push_back(record);
  }
}

/**
 * @brief set the current size of buffer
 */
void StdoutBackendLogger::Commit(){
  {
    std::lock_guard<std::mutex> lock(stdout_buffer_mutex);
    commit_offset = stdout_buffer.size();
  }
}

/**
 * @brief Get the LogRecord with offset
 * @param offset
 */
LogRecord StdoutBackendLogger::GetLogRecord(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(stdout_buffer_mutex);
    assert(offset < stdout_buffer.size());
    return stdout_buffer[offset];
  }
}

/**
 * @brief truncate buffer with commit_offset
 * @param offset
 */
void StdoutBackendLogger::Truncate(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(stdout_buffer_mutex);
    stdout_buffer.erase(stdout_buffer.begin(), stdout_buffer.begin()+offset);

    // It will be updated larger than 0 if we update commit_offset during the
    // flush in frontend logger
    commit_offset -= offset;
  }
}

}  // namespace logging
}  // namespace peloton
