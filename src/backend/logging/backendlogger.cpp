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
#include "backend/logging/loggers/ariesbackendlogger.h"
#include "backend/logging/loggers/pelotonbackendlogger.h"

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
      //backendLogger = StdoutBackendLogger::GetInstance();
    }break;

    case LOGGING_TYPE_ARIES:{
      backendLogger = AriesBackendLogger::GetInstance();
    }break;

    case LOGGING_TYPE_PELOTON:{
      backendLogger = PelotonBackendLogger::GetInstance();
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
 * @brief Get the LogRecord with offset
 * @param offset
 */
LogRecord* BackendLogger::GetLogRecord(oid_t offset){
  std::lock_guard<std::mutex> lock(local_queue_mutex);
  assert(offset < local_queue.size());
  return local_queue[offset];
}

/**
 * @brief if we still have log record in local queue or waiting flush, 
 * @return true otherwise false
 */
bool BackendLogger::IsWaitFlush(void) const{
  if( GetLocalQueueSize() > 0 || wait_flush ){
    return true;
  }else{
    return false;
  }
}

bool BackendLogger::IsAddedFrontend(void) const{
  return added_in_frontend;
}

void BackendLogger::AddedFrontend(void) {
  added_in_frontend = true;
}

}  // namespace logging
}
