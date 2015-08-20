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
    std::lock_guard<std::mutex> lock(aries_local_queue_mutex);
    record->Serialize();
    aries_local_queue.push_back(record);
  }
  Commit();
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void AriesBackendLogger::Delete(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(aries_local_queue_mutex);
    record->Serialize();
    aries_local_queue.push_back(record);
  }
  Commit();
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void AriesBackendLogger::Update(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(aries_local_queue_mutex);
    record->Serialize();
    aries_local_queue.push_back(record);
  }
  Commit();
}

/**
 * @brief set the current size of local_queue
 */
void AriesBackendLogger::Commit(){
  {
    std::lock_guard<std::mutex> lock(aries_local_queue_mutex);
    commit_offset = aries_local_queue.size();
  }
}

/**
 * @brief truncate local_queue with commit_offset
 * @param offset
 */
void AriesBackendLogger::Truncate(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(aries_local_queue_mutex);

    if(commit_offset == offset){
      aries_local_queue.clear();
    }else{
      aries_local_queue.erase(aries_local_queue.begin(), aries_local_queue.begin()+offset);
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
LogRecord* AriesBackendLogger::GetLogRecord(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(aries_local_queue_mutex);
    assert(offset < aries_local_queue.size());
    return aries_local_queue[offset];
  }
}

}  // namespace logging
}  // namespace peloton
