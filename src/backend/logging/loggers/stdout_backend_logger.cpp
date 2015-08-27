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

#include "backend/logging/loggers/stdout_backend_logger.h"

namespace peloton {
namespace logging {

StdoutBackendLogger* StdoutBackendLogger::GetInstance(){
  static thread_local StdoutBackendLogger pInstance; 
  return &pInstance;
}

/**
 * @brief Insert LogRecord
 * @param log record 
 */
void StdoutBackendLogger::Insert(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    local_queue.push_back(record);
  }
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void StdoutBackendLogger::Delete(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    local_queue.push_back(record);
  }
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void StdoutBackendLogger::Update(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    local_queue.push_back(record);
  }
}

}  // namespace logging
}  // namespace peloton
