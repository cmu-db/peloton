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

#include "backend/logging/log_manager.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the singleton log manager instance
 */
LogManager& LogManager::GetInstance(){
  static LogManager log_manager;
  return log_manager;
}

/**
 * @brief Standby logging based on logging type
 *  and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StartStandbyMode(LoggingType logging_type){
  assert(logging_type != LOGGING_TYPE_INVALID);

  FrontendLogger* frontend_logger = nullptr;
  bool frontend_exists = false;

  // Check whether the frontend logger that has the same logging type
  // as the given logging type exists or not ?
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);

    // find frontend logger inside log manager
    frontend_logger = GetFrontendLogger(logging_type);

    // If frontend logger doesn't exist
    if( frontend_logger == nullptr ){

      frontend_logger = FrontendLogger::GetFrontendLogger(logging_type);
      frontend_loggers.push_back(frontend_logger);

      // We are adding a peloton frontend logger
      if(logging_type == LOGGING_TYPE_PELOTON)
        has_peloton_frontend_logger = true;

      // Set has_frontend_logger
      if(frontend_loggers.empty() == false)
        has_frontend_logger = true;

    } else{
      frontend_exists = true;
    }
  }

  // Toggle status in log manager map
  SetLoggingStatus(logging_type, LOGGING_STATUS_TYPE_STANDBY);

  if(frontend_exists == false){
    // Launch the frontend logger's main loop
    frontend_logger->MainLoop();
  } else{
    LOG_ERROR("A frontend logger with the same LoggingType(%s) already exists\n",
              LoggingTypeToString(logging_type).c_str());
  }

}

void LogManager::StartRecoveryMode(LoggingType logging_type) {
  assert(logging_type != LOGGING_TYPE_INVALID);

  // Toggle the status after STANDBY
  SetLoggingStatus(logging_type, LOGGING_STATUS_TYPE_RECOVERY);
}

bool LogManager::IsInLoggingMode(LoggingType logging_type){
  assert(logging_type != LOGGING_TYPE_INVALID);

  // Fast check
  if(has_frontend_logger == false)
    return false;

  // Slow check for specific logging_type
  if(GetStatus(logging_type) == LOGGING_STATUS_TYPE_LOGGING)
    return true;
  else
    return false;
}

void LogManager::TerminateLoggingMode(LoggingType logging_type){
  assert(logging_type != LOGGING_TYPE_INVALID);

  SetLoggingStatus(logging_type, LOGGING_STATUS_TYPE_TERMINATE);

  // notify frontend to exit current waiting
  NotifyFrontendLogger(logging_type);

  // We set the frontend logger status to Terminate
  // And, then we wait for the transition to Sleep mode
  WaitForMode(LOGGING_STATUS_TYPE_SLEEP, true, logging_type);
}

void LogManager::WaitForMode(LoggingStatus logging_status,
                             bool is_equal,
                             LoggingType logging_type) {

  // check logging type
  assert(logging_type != LOGGING_TYPE_INVALID);

  // wait for mode change
  {
    std::unique_lock<std::mutex> wait_lock(logging_status_mutex);

    while(logging_statuses.find(logging_type) == logging_statuses.end() ||
        (logging_statuses.find(logging_type) != logging_statuses.end() &&
            ((!is_equal && logging_statuses[logging_type] == logging_status) ||
                (is_equal && logging_statuses[logging_type] != logging_status)))){
      logging_status_cv.wait(wait_lock);
    }
  }

}

/**
 * @brief stopping logging
 * Disconnect backend loggers and frontend logger from log manager
 */
bool LogManager::EndLogging(LoggingType logging_type ){
  assert(logging_type != LOGGING_TYPE_INVALID);

  // Wait if current status is recovery
  WaitForMode(LOGGING_STATUS_TYPE_RECOVERY, false, logging_type);

  LOG_INFO("Wait until frontend logger(%s) escapes main loop..",
           LoggingStatusToString(GetStatus(logging_type)).c_str());

  // Wait for the frontend logger to enter sleep mode
  TerminateLoggingMode(logging_type);

  LOG_INFO("Escaped from MainLoop(%s)",
           LoggingStatusToString(GetStatus(logging_type)).c_str());

  // Remove the frontend logger
  if( RemoveFrontendLogger(logging_type) ) {
    ResetLoggingStatusMap(logging_type);
    LOG_INFO("%s has been terminated successfully",
             LoggingTypeToString(logging_type).c_str());
    return true;
  }

  return false;
}

//===--------------------------------------------------------------------===//
// Utility Functions
//===--------------------------------------------------------------------===//

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger* LogManager::GetBackendLogger(LoggingType logging_type){
  assert(logging_type != LOGGING_TYPE_INVALID);

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
      if (!backend_logger->IsConnectedToFrontend()) {
        frontend_logger->AddBackendLogger(backend_logger);
      }
    }
  }

  if( frontend_logger == nullptr){
    LOG_ERROR("%s frontend logger doesn't exist!!\n",LoggingTypeToString(logging_type).c_str());
  }

  return backend_logger;
}

bool LogManager::RemoveBackendLogger(BackendLogger* backend_logger, LoggingType logging_type){
  assert(logging_type != LOGGING_TYPE_INVALID);

  bool status = false;

  // Check whether the corresponding frontend logger exists or not
  // if so, remove backend logger otherwise return false
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    FrontendLogger* frontend_logger = GetFrontendLogger(logging_type);
    // If frontend logger exists
    if( frontend_logger != nullptr){
      status = frontend_logger->RemoveBackendLogger(backend_logger);
    }
  }

  return status;
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

bool LogManager::RemoveFrontendLogger(LoggingType logging_type ){
  //Erase frontend logger from frontend_loggers as well
  {
    oid_t offset=0;
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);

    // Clean up the frontend logger
    for(auto frontend_logger : frontend_loggers){
      if( frontend_logger->GetLoggingType() == logging_type){
        delete frontend_logger;

        // We are removing a peloton frontend logger
        if(logging_type == LOGGING_TYPE_PELOTON)
          has_peloton_frontend_logger = false;
        break;
      }else{
        offset++;
      }
    }

    // Set has_frontend_logger
    if(frontend_loggers.empty() == true)
      has_frontend_logger = false;

    // Remove the entry in the frontend logger list
    if( offset >= frontend_loggers.size()){
      LOG_WARN("%s isn't running", LoggingTypeToString(logging_type).c_str());
      return false;
    }else{
      frontend_loggers.erase(frontend_loggers.begin()+offset);
      return true;
    }
  }
}

void LogManager::ResetLoggingStatusMap(LoggingType logging_type ) {

  // Remove status for the specific logging type
  {
    std::lock_guard<std::mutex> lock(logging_status_mutex);
    std::map<LoggingType,LoggingStatus>::iterator it;
    it = logging_statuses.find(logging_type);

    if (it != logging_statuses.end()){
      logging_statuses.erase(it);
    }
    // in case any threads are waiting...
    logging_status_cv.notify_all();
  }

}

size_t LogManager::ActiveFrontendLoggerCount(void) {
  std::lock_guard<std::mutex> lock(frontend_logger_mutex);
  return frontend_loggers.size();
}

/**
 * @brief mark Peloton is ready, so that frontend logger can start logging
 */
LoggingStatus LogManager::GetStatus(LoggingType logging_type) {
  assert(logging_type != LOGGING_TYPE_INVALID);

  // Get the status from the map
  {
    std::lock_guard<std::mutex> lock(logging_status_mutex);

    std::map<LoggingType,LoggingStatus>::iterator it;

    it = logging_statuses.find(logging_type);
    if (it != logging_statuses.end()){
      return it->second;
    } else {
      return LOGGING_STATUS_TYPE_INVALID;
    }
  }

}

void LogManager::SetLoggingStatus(LoggingType logging_type, LoggingStatus logging_status){
  assert(logging_type != LOGGING_TYPE_INVALID);

  // Set the status in the log manager map
  {
    std::lock_guard<std::mutex> lock(logging_status_mutex);

    std::map<LoggingType,LoggingStatus>::iterator it;
    it = logging_statuses.find(logging_type);

    if (it != logging_statuses.end()){
      // Ensure that we cannot change to previous status in the transition diagram
      // i.e., we can change status only in this direction:
      // standby -> recovery -> logging -> terminate -> sleep
      if( logging_statuses[logging_type] < logging_status){
        logging_statuses[logging_type] = logging_status;
      }
    }else{
      logging_statuses.insert(std::make_pair(logging_type, logging_status));
    }

    // notify everyone about the status change
    logging_status_cv.notify_all();
  }
}

void LogManager::NotifyFrontendLogger(LoggingType logging_type, bool newLog) {
  std::lock_guard<std::mutex> lock(frontend_logger_mutex);
  FrontendLogger *frontend = GetFrontendLogger(logging_type);
  if (frontend != nullptr) {
    frontend->NotifyFrontend(newLog);
  }
}

void LogManager::SetTestRedoAllLogs(LoggingType logging_type, bool test_redo_all) {
  {
    std::lock_guard<std::mutex> lock(frontend_logger_mutex);
    FrontendLogger *frontend = GetFrontendLogger(logging_type);

    frontend->SetTestRedoAllLogs(test_redo_all);
  }
}

void LogManager::SetLogFileName(std::string log_file) {
  log_file_name = log_file;
}

// XXX change to read configuration file
std::string LogManager::GetLogFileName(void) {

  // Check if we need to build a log file name
  if(log_file_name.empty()) {

    // If peloton_log_directory is specified
    if(peloton_log_directory != nullptr) {
      log_file_name = std::string(peloton_log_directory) + "/" + "peloton.log";
    }
    // Else save it in tmp directory
    else {
      log_file_name = "/tmp/peloton.log";
    }

  }

  return log_file_name;
}


}  // namespace logging
}  // namespace peloton
