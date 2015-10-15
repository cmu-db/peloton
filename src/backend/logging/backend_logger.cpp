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

#include "backend_logger.h"

#include "backend/common/logger.h"
#include "backend/logging/loggers/aries_backend_logger.h"
#include "backend/logging/loggers/peloton_backend_logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the backend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* BackendLogger::GetBackendLogger(LoggingType logging_type){
  BackendLogger* backendLogger;

  switch(logging_type){
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
  wait_for_flushing = false;
}

/**
 * @brief set the wait flush to true and truncate local_queue with commit_offset
 * @param offset
 */
void BackendLogger::TruncateLocalQueue(oid_t offset){

  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);

    // cleanup the queue
    local_queue.erase(local_queue.begin(),
                      local_queue.begin()+offset);

    // let's wait for the frontend logger to flush !
    // the frontend logger will call our Commit to reset it.
    wait_for_flushing = true;
  }
}

/**
 * @brief Get the LogRecord with offset
 * @param offset
 */
LogRecord* BackendLogger::GetLogRecord(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    assert(offset < local_queue.size());
    return local_queue[offset];
  }
}

/**
 * @brief if we still have log record in local queue or waiting for flushing
 * @return true otherwise false
 */
bool BackendLogger::IsWaitingForFlushing(void) const{

  // Sometimes, the backend logger has some log records even if
  // wait_for_flushing is false.
  // For example, if backend logger enqueues the log record right after
  // frontend logger collect data and not truncated yet.
  if(wait_for_flushing || GetLocalQueueSize() > 0) {
    return true;
  } else {
    return false;
  }
}

bool BackendLogger::IsConnectedToFrontend(void) const{
  return connected_to_frontend;
}

void BackendLogger::SetConnectedToFrontend(bool isConnected) {
  connected_to_frontend = isConnected;
}

}  // namespace logging
}
