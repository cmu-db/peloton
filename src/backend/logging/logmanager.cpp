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
  bool frontend_exists = false;

  // Check whether the frontend logger that has the same logging type with given logging type exists or not
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);

    // find frontend logger inside log manager
    frontend_logger = GetFrontendLogger(logging_type);

    // If frontend logger doesn't exist
    if( frontend_logger == nullptr ){
      frontend_logger = FrontendLogger::GetFrontendLogger(logging_type);
      frontend_loggers.push_back(frontend_logger);
    }else{
      frontend_exists = true;
    }
  }

  // If frontend logger exists
  if( frontend_exists ){
    LOG_ERROR("The same LoggingType(%s) FrontendLogger already exists!!\n",LoggingTypeToString(logging_type).c_str());
  }else{
      frontend_logger->MainLoop();
  }
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* LogManager::GetBackendLogger(LoggingType logging_type){
  FrontendLogger* frontend_logger = nullptr;
  BackendLogger* backend_logger =  nullptr;

  // Check whether the corresponding frontend logger exists or not
  // if so, create backend logger and store it in frontend logger
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    
    frontend_logger = GetFrontendLogger(logging_type);


    // If frontend logger exists
    if( frontend_logger != nullptr){
      backend_logger = BackendLogger::GetBackendLogger(logging_type);
      frontend_logger->AddBackendLogger(backend_logger);
    }
  }

  if( frontend_logger == nullptr){
    LOG_ERROR("%s frontend logger doesn't exist!!\n",LoggingTypeToString(logging_type).c_str());
  }
  return backend_logger;
}

/**
 * @brief Restore database based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::Restore(LoggingType logging_type){
  FrontendLogger* frontend_logger = nullptr;
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    frontend_logger = GetFrontendLogger(logging_type);
  }
  if( frontend_logger == nullptr || logging_type == LOGGING_TYPE_STDOUT ){
    LOG_ERROR("Restore is failed, LoggingType is %s",LoggingTypeToString(logging_type).c_str());
  }else{
    frontend_logger->Restore();
  }
}

/**
 * @brief Find Frontend Logger in Log Manager
 * @param logging type can be stdout(debug), aries, peloton
 * @return the corresponding frontend logger otherwise nullptr
 */
FrontendLogger* LogManager::GetFrontendLogger(LoggingType logging_type){
  for(auto frontend_logger : frontend_loggers){
    if( frontend_logger->GetLoggingType() == logging_type){
      return frontend_logger;
    }
  }
  return nullptr;
}

}  // namespace logging
}  // namespace peloton
