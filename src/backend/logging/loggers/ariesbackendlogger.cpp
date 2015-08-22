/*-------------------------------------------------------------------------
 *
 * ariesbackendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesbackendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/ariesbackendlogger.h"

#include <iostream>

namespace peloton {
namespace logging {

AriesBackendLogger* AriesBackendLogger::GetInstance(){
  static  thread_local AriesBackendLogger pInstance; 
  return &pInstance;
}

/**
 * @brief Insert LogRecord
 * @param log record 
 */
void AriesBackendLogger::Insert(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void AriesBackendLogger::Delete(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void AriesBackendLogger::Update(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
}

}  // namespace logging
}  // namespace peloton
