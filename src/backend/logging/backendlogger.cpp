/*-------------------------------------------------------------------------
 *
 * backendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/backendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/backendlogger.h"
#include "backend/logging/loggers/stdoutbackendlogger.h"
#include "backend/logging/loggers/ariesbackendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the backend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* BackendLogger::GetBackendLogger(LoggingType logging_type){
  BackendLogger* backendLogger;

  switch(logging_type){
    case LOGGING_TYPE_STDOUT:{
      backendLogger = StdoutBackendLogger::GetInstance();
    }break;

    case LOGGING_TYPE_ARIES:{
      backendLogger = AriesBackendLogger::GetInstance();
    }break;

    case LOGGING_TYPE_PELOTON:{
//      backendLogger = new PelotonBackendLogger();
    }break;

    default:
    LOG_ERROR("Unsupported backend logger type");
    break;
  }

  return backendLogger;
}

/**
 * @brief set the wait flush to false
 */
void BackendLogger::Commit(void){
  wait_flush = false;
}

/**
 * @brief set the wait flush to true and truncate local_queue with commit_offset
 * @param offset
 */
void BackendLogger::Truncate(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);

    wait_flush = true;

    if(commit_offset == offset){
      local_queue.clear();
    }else{
      local_queue.erase(local_queue.begin(), local_queue.begin()+offset);
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
LogRecord* BackendLogger::GetLogRecord(oid_t offset){
  assert(offset < local_queue.size());
  return local_queue[offset];
}

/**
 * @brief Get the local queue size
 * @return local queue size
 */
size_t BackendLogger::GetLocalQueueSize(void) const {
  return local_queue.size();
}

/**
 * @brief if we still have log record in local queue or waiting flush, 
 * @return true otherwise false
 */
bool BackendLogger::IsWaitFlush(void) const {
  if( GetLocalQueueSize() > 0 || wait_flush ){
    return true;
  }else{
    return false;
  }
}

}  // namespace logging
}
