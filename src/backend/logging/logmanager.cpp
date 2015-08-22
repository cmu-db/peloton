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
 * @brief Return the log manager instance
 */
LogManager& LogManager::GetInstance(){
  static LogManager logManager;
  return logManager;
}

void LogManager::SetMainLoggingType(LoggingType logging_type){
  MainLoggingType = logging_type;
}

LoggingType LogManager::GetMainLoggingType(void){
  return MainLoggingType;
}

/**
 * @brief Standty logging based on logging type
 *  and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StandbyLogging(LoggingType logging_type){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }

  SetLoggingStatus(logging_type, LOGGING_STATUS_TYPE_STANDBY);

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

void LogManager::StartLogging(LoggingType logging_type){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }
  SetLoggingStatus(logging_type, LOGGING_STATUS_TYPE_RECOVERY);
}

/**
 * @brief stopping logging 
 */
bool LogManager::EndLogging(LoggingType logging_type ){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }

  // Wait if current status is recovery
  while(GetLoggingStatus(logging_type) == LOGGING_STATUS_TYPE_RECOVERY){}

  LOG_INFO("Wait until frontend logger(%s) escapes main loop..", LoggingStatusToString(GetLoggingStatus(logging_type)).c_str());

  while(1){
    sleep(1);
    MakeItSleepy();
    if( GetLoggingStatus(logging_type) == LOGGING_STATUS_TYPE_SLEEP) break;
  }

  LOG_INFO("Escaped from MainLoop(%s)", LoggingStatusToString(GetLoggingStatus(logging_type)).c_str());

  if( RemoveFrontend(logging_type) ){
    ResetLoggingStatus(logging_type);
    LOG_INFO("%s has been terminated successfully", LoggingTypeToString(logging_type).c_str());
    return true;
  }
  return false;
}

bool LogManager::IsReadyToLogging(LoggingType logging_type){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    if( logging_type == LOGGING_TYPE_INVALID)
      return false;
  }

  auto logging_status = GetLoggingStatus(logging_type);
  if( logging_status == LOGGING_STATUS_TYPE_ONGOING)
    return true;
  else
    return false;
}

size_t LogManager::ActiveFrontendLoggerCount(void) const{
  return frontend_loggers.size();
}

/**
 * @brief mark Peloton is ready, so that frontend logger can start logging
 */
LoggingStatus LogManager::GetLoggingStatus(LoggingType logging_type){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }

  {
    std::lock_guard<std::mutex> lock(logging_status_mutex);

    std::map<LoggingType,LoggingStatus>::iterator it;

    it = logging_statuses.find(logging_type);
    if (it != logging_statuses.end()){
      return it->second;
    }else{
      return LOGGING_STATUS_TYPE_INVALID;
    }
  }
}

void LogManager::SetLoggingStatus(LoggingType logging_type, LoggingStatus logging_status){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }

  {
    std::lock_guard<std::mutex> lock(logging_status_mutex);

    std::map<LoggingType,LoggingStatus>::iterator it; 
    it = logging_statuses.find(logging_type);

    if (it != logging_statuses.end()){
       // Cannot change to previous status
       // For example,  we can change status only in follow direction, standby->recovery->ongoing->terminate->sleep 
       if( logging_statuses[logging_type] < logging_status){
         logging_statuses[logging_type] = logging_status;
       }
    }else{
      logging_statuses.insert(std::make_pair(logging_type, logging_status));
    }
  }
}

void LogManager::MakeItSleepy(LoggingType logging_type){
  if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }

  SetLoggingStatus(logging_type, LOGGING_STATUS_TYPE_TERMINATE);
}


/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* LogManager::GetBackendLogger(LoggingType logging_type){
if( logging_type == LOGGING_TYPE_INVALID){
    logging_type = MainLoggingType;
    assert(logging_type);
  }

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
      if( backend_logger->IsAddedFrontend() == false){
        frontend_logger->AddBackendLogger(backend_logger);
        backend_logger->AddedFrontend();
      }
    }
  }

  if( frontend_logger == nullptr){
    LOG_ERROR("%s frontend logger doesn't exist!!\n",LoggingTypeToString(logging_type).c_str());
  }
  return backend_logger;
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

bool LogManager::RemoveFrontend(LoggingType logging_type ){
  //Erase frontend logger from frontend_loggers as well
  {
    oid_t offset=0;
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    for(auto frontend_logger : frontend_loggers){
      if( frontend_logger->GetLoggingType() == logging_type){
        delete frontend_logger;
        break;
      }else{
        offset++;
      }
    }
    if( offset >= frontend_loggers.size()){
      LOG_WARN("%s isn't running", LoggingTypeToString(logging_type).c_str());
      return false;
    }else{
      frontend_loggers.erase(frontend_loggers.begin()+offset);
      return true;
    }
  }
}

void LogManager::ResetLoggingStatus(LoggingType logging_type ){
  // Remove status 
  {
    std::map<LoggingType,LoggingStatus>::iterator it; 
    it = logging_statuses.find(logging_type);

    if (it != logging_statuses.end()){
      logging_statuses.erase(it);
    }
  }
  // Reset MainLoggingType 
  MainLoggingType = LOGGING_TYPE_INVALID;
}

}  // namespace logging
}  // namespace peloton
