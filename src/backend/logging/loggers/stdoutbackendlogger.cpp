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

StdoutBackendLogger* StdoutBackendLogger::GetInstance(){
  static StdoutBackendLogger pInstance; 
  return &pInstance;
}

/**
 * @brief Recording log record
 * @param log record 
 */
void StdoutBackendLogger::Log(LogRecord record){
  {
    std::lock_guard<std::mutex> lock(stdout_local_queue_mutex);
    stdout_local_queue.push_back(record);
  }
  //FIXME :: Commit everytime for testing
  Commit();
}

/**
 * @brief set the current size of local_queue
 */
void StdoutBackendLogger::Commit(){
  {
    std::lock_guard<std::mutex> lock(stdout_local_queue_mutex);
    commit_offset = stdout_local_queue.size();
  }
}

/**
 * @brief truncate local_queue with commit_offset
 * @param offset
 */
void StdoutBackendLogger::Truncate(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(stdout_local_queue_mutex);

    if(commit_offset == offset){
      stdout_local_queue.clear();
    }else{
      stdout_local_queue.erase(stdout_local_queue.begin(), stdout_local_queue.begin()+offset);
    }

    // It will be updated larger than 0 if we update commit_offset during the
    // flush in frontend logger
    commit_offset -= offset;
  }
}

/**
 * @brief Get the LogRecord with offset
 * @param offset
 */
LogRecord StdoutBackendLogger::GetLogRecord(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(stdout_local_queue_mutex);
    assert(offset < stdout_local_queue.size());
    return stdout_local_queue[offset];
  }
}

}  // namespace logging
}  // namespace peloton
