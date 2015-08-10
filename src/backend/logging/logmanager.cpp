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
 * @brief Start logging based on logging type
 *  and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StartLogging(LoggingType logging_type){

  FrontendLogger* frontend_logger = nullptr;
  bool is_frontend_exist = false;

  // Check whether the frontend logger that has the same logging type with given logging type exists or not
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    for(auto frontend_logger : frontend_loggers){
      if( frontend_logger->GetLoggingType() == logging_type){
        is_frontend_exist = true;
        break;
      }
    }

    // Create the frontend logger 
    if( !is_frontend_exist ){
      frontend_logger = FrontendLogger::GetFrontendLogger(logging_type);
      frontend_loggers.push_back(frontend_logger);
    }
  }

  if( is_frontend_exist ){
    LOG_ERROR("The same LoggingType(%s) FrontendLogger already exists!!\n",LoggingTypeToString(logging_type).c_str());
  }else{
      frontend_logger->MainLoop();
  }
}

/**
 * @brief Restore database based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::Restore(LoggingType logging_type){
  // read .. ?
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* LogManager::GetBackendLogger(LoggingType logging_type){
  BackendLogger* backend_logger =  nullptr;
  bool is_frontend_exist = false;

  // Check whether the corresponding frontend logger exists or not
  // if so, create backend logger and store it in frontend logger
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    for(auto frontend_logger : frontend_loggers){
      if(frontend_logger->GetLoggingType() == logging_type){
        is_frontend_exist = true;

        backend_logger = BackendLogger::GetBackendLogger(logging_type);
        frontend_logger->AddBackendLogger(backend_logger);
        break;
      }
    }
  }

  if( is_frontend_exist == false){
    LOG_ERROR("%s frontend logger isn't exist!!\n",LoggingTypeToString(logging_type).c_str());
  }
  return backend_logger;
}

}  // namespace logging
}  // namespace peloton
