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
  static AriesBackendLogger pInstance; 
  return &pInstance;
}

/**
 * @brief Recording log record
 * @param log record 
 */
void AriesBackendLogger::Log(LogRecord record){
  {
    std::lock_guard<std::mutex> lock(aries_buffer_mutex);
    aries_buffer.push_back(record);
  }
  //FIXME :: Commit everytime for testing
  Commit();
}

/**
 * @brief set the current size of buffer
 */
void AriesBackendLogger::Commit(){
  {
    std::lock_guard<std::mutex> lock(aries_buffer_mutex);
    commit_offset = aries_buffer.size();
  }
}

/**
 * @brief truncate buffer with commit_offset
 * @param offset
 */
void AriesBackendLogger::Truncate(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(aries_buffer_mutex);

    if(commit_offset == offset){
      aries_buffer.clear();
    }else{
      aries_buffer.erase(aries_buffer.begin(), aries_buffer.begin()+offset);
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
LogRecord AriesBackendLogger::GetLogRecord(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(aries_buffer_mutex);
    assert(offset < aries_buffer.size());
    return aries_buffer[offset];
  }
}

}  // namespace logging
}  // namespace peloton
