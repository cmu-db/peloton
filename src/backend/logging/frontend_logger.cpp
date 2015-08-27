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

#include "backend/common/logger.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/aries_frontend_logger.h"
#include "backend/logging/loggers/peloton_frontend_logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the frontend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
FrontendLogger* FrontendLogger::GetFrontendLogger(LoggingType logging_type){
  FrontendLogger* frontendLogger;

  switch(logging_type){
    case LOGGING_TYPE_ARIES:{
      frontendLogger = new AriesFrontendLogger();
    }break;

    case LOGGING_TYPE_PELOTON:{
      frontendLogger = new PelotonFrontendLogger();
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
void FrontendLogger::AddBackendLogger(BackendLogger* backend_logger){
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    backend_loggers.push_back(backend_logger);
  }
}

/**
 * @brief Get backend loggers
 * @return the backend loggers
 */
std::vector<BackendLogger*> FrontendLogger::GetBackendLoggers(){
  return backend_loggers;
}

bool FrontendLogger::RemoveBackendLogger(BackendLogger* _backend_logger){

  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    oid_t offset=0;

    for(auto backend_logger : backend_loggers){
      if( backend_logger == _backend_logger){
        break;
      }else{
        offset++;
      }
    }

    assert(offset<backend_loggers.size());
    backend_loggers.erase(backend_loggers.begin()+offset);
  }

  return true;
}

}  // namespace logging
}
