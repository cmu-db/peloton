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
 * @brief Start logging based on logger type
    and store it into the vector
 * @param logger type can be stdout(debug), aries, peloton
 */
void LogManager::StartLogging(LoggerType logger_type){
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    // TODO :: Check whether it already has frontend logger or not
    FrontendLogger* frontend_logger = FrontendLogger::GetFrontendLogger(logger_type);
    frontend_loggers.push_back(frontend_logger);
    frontend_logger->MainLoop();
  }
}

/**
 * @brief Return the backend logger based on logger type
    and store it into the vector
 * @param logger type can be stdout(debug), aries, peloton
 */
BackendLogger* LogManager::GetBackendLogger(LoggerType logger_type){
  BackendLogger* backend_logger = BackendLogger::GetBackendLogger(logger_type);
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    backend_loggers.push_back(backend_logger);
  }
  return backend_logger;
}

///** FIXME :: Do we need this function?
// * @brief Return the logger based on logger type and logging_type
// * @param logger type can be stdout(debug), aries, peloton
// * @param logging type can be frontend or backend
//    frontend logger is only one for each logger type
// */
////TODO :: Do not create new instance, just return existing object
//Logger* Logger::GetLogger(LoggerType logger_type,
//                          LoggingType logging_type){
//  Logger* logger;
//
//  // it should be changed to use vector. .
//  // i mean look over vector..
//  switch(logging_type){
//    case LOGGING_TYPE_FRONTEND:{
//      logger = GetFrontendLogger(logger_type);
//    }break;
//
//    case LOGGING_TYPE_BACKEND:{
//      logger = GetBackendLogger(logger_type);
//    }break;
//
//    default:
//    LOG_ERROR("Unsupported logging type");
//    break;
//  }
//
//  return logger;
//}

}  // namespace logging
}  // namespace peloton
