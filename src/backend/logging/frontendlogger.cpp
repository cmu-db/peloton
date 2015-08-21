/*-------------------------------------------------------------------------
 *
 * frontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/frontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/frontendlogger.h"
#include "backend/logging/loggers/stdoutfrontendlogger.h"
#include "backend/logging/loggers/ariesfrontendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the frontend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
FrontendLogger* FrontendLogger::GetFrontendLogger(LoggingType logging_type){
  FrontendLogger* frontendLogger;

  switch(logging_type){
    case LOGGING_TYPE_STDOUT:{
      frontendLogger = new StdoutFrontendLogger();
    }break;

    case LOGGING_TYPE_ARIES:{
      frontendLogger = new AriesFrontendLogger();
    }break;

    case LOGGING_TYPE_PELOTON:{
//      frontendLogger = new PelotonFrontendLogger();
    }break;

    default:
    LOG_ERROR("Unsupported frontend logging type");
    break;
  }

  return frontendLogger;
}

/**
 * @brief Store backend logger
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger* _backend_logger){
  {
    bool already_exist = false;
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    for( auto backend_logger : backend_loggers){
      if( backend_logger == _backend_logger){
        already_exist = true;
        break;
      }
    }
    if( already_exist == false){
      printf("AddBackendLogger\n");
      backend_loggers.push_back(_backend_logger);
    }
  }
}

/**
 * @brief Get backend loggers
 * @return the backend loggers
 */
std::vector<BackendLogger*> FrontendLogger::GetBackendLoggers(){
    return backend_loggers;
}

}  // namespace logging
}
