/*-------------------------------------------------------------------------
 *
 * logmanager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logmanager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/common/logger.h"
#include "backend/logging/logmanager.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the global unique instance for log manager
 */
std::shared_ptr<LogManager>& LogManager::GetInstance(){
  std::lock_guard<std::mutex> lock(logManager_mutex);
  static std::shared_ptr<LogManager> logManager(new LogManager());
  return logManager;
}

/**
 * @brief Start logging based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StartLogging(LoggingType logging_type){
  FrontendLogger* frontend_logger;
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    for(auto frontend_logger : frontend_loggers){
      if( frontend_logger->GetLoggingType() == logging_type){
        LOG_ERROR("(%s)FrontendLogger already exists!!\n",LoggingTypeToString(logging_type).c_str());
      }
    }
    frontend_logger = FrontendLogger::GetFrontendLogger(logging_type);
    frontend_loggers.push_back(frontend_logger);
  }
  frontend_logger->MainLoop();
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* LogManager::GetBackendLogger(LoggingType logging_type){
  //TODO :: check whether frontend logger is running or not
  // and set the frontend logger to corresponding backend server
  BackendLogger* backend_logger = BackendLogger::GetBackendLogger(logging_type);
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    backend_loggers.push_back(backend_logger);
  }
  return backend_logger;
}

}  // namespace logging
}  // namespace peloton
