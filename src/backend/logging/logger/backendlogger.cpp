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

#include "backend/logging/logger/backendlogger.h"
#include "backend/logging/logger/stdoutbackendlogger.h"

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
      backendLogger = new StdoutBackendLogger();
    }break;

//    case LOGGER_TYPE_ARIES:{
//      backendLogger = new AriesBackendLogger();
//    }break;
//
//    case LOGGER_TYPE_PELOTON:{
//      backendLogger = new PelotonBackendLogger();
//    }break;

    default:
    LOG_ERROR("Unsupported backend logger type");
    break;
  }

  return backendLogger;
}

/**
 * @brief Return uniqe backend logger id
 */
oid_t BackendLogger::GetBackendLoggerId() const{
  return backend_id;
}

}  // namespace logging
}
