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
//      backendLogger = new AriesBackendLogger();
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

oid_t BackendLogger::GetCommitOffset() const{
  return commit_offset;
}

}  // namespace logging
}
