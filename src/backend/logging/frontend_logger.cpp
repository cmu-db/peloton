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
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/aries_frontend_logger.h"
#include "backend/logging/loggers/peloton_frontend_logger.h"

namespace peloton {
namespace logging {

/** * @brief Return the frontend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
FrontendLogger* FrontendLogger::GetFrontendLogger(LoggingType logging_type){
  FrontendLogger* frontendLogger = nullptr;

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
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {

  auto& logManager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Standby Mode");

  // Standby before we need to do RECOVERY
  logManager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, false);

  // Do recovery if we can, otherwise terminate
  switch(logManager.GetStatus()){
    case LOGGING_STATUS_TYPE_RECOVERY:{
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      DoRecovery();

      // Now, enter LOGGING mode
      logManager.SetLoggingStatus(GetLoggingType(), LOGGING_STATUS_TYPE_LOGGING);

      break;
    }

    case LOGGING_STATUS_TYPE_LOGGING:{
      LOG_TRACE("Frontendlogger] Logging Mode");
    }
    break;

    default:
    break;
  }

  /////////////////////////////////////////////////////////////////////
  // LOGGING MODE
  /////////////////////////////////////////////////////////////////////

  // Periodically, wake up and do logging
  while(logManager.GetStatus(GetLoggingType()) == LOGGING_STATUS_TYPE_LOGGING){

    // Collect LogRecords from all backend loggers
    CollectLogRecord();

    // Flush the data to the file
    Flush();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  {
    std::lock_guard<std::mutex> lock(backend_notify_mutex);

    // force the last check to be done without waiting
    log_collect_request = true;
  }

  // flush any remaining log records
  CollectLogRecord();
  Flush();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Sleep Mode");

  //Setting frontend logger status to sleep
  logManager.SetLoggingStatus(GetLoggingType(), 
                              LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Notify frontend logger to start collect records
 */
void FrontendLogger::NotifyFrontend(bool hasNewLog) {
  if (hasNewLog) {
    std::lock_guard<std::mutex> lock(backend_notify_mutex);
    if (log_collect_request == false) {
      log_collect_request = true;
    }
    // Only when new logs appear,
    // we need lock backend_notify_mutex and notify
    backend_notify_cv.notify_one();
  } else {
    // No need to lock mutex
    backend_notify_cv.notify_one();
  }
}

/**
 * @brief Collect the LogRecord from BackendLoggers
 */
void FrontendLogger::CollectLogRecord() {

  std::unique_lock<std::mutex> wait_lock(backend_notify_mutex);
  /*
   * Don't use "while(!new_log_available)", we want the frontend check all
   * backend periodically even no backend notifies. So that large txn will
   * can submit it's logs piece by piece instead of a huge submission when
   * the txn is committed.
   */
  if (!log_collect_request) {
    backend_notify_cv.wait_for(wait_lock,
                             std::chrono::seconds(wait_timeout)); // timeout
  }

  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    // Look at the commit mark of the backend loggers of the current frontend logger
    for( auto backend_logger : backend_loggers){
      auto local_queue_size = backend_logger->GetLocalQueueSize();

      // Skip current backend_logger, nothing to do
      if(local_queue_size == 0 ) continue; 

      for(oid_t log_record_itr=0; log_record_itr<local_queue_size; log_record_itr++){
        // Shallow copy the log record from backend_logger to here
        global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
      }

      // truncate the local queue 
      backend_logger->TruncateLocalQueue(local_queue_size);
    }
  }
  log_collect_request = false;
}

/**
 * @brief Store backend logger
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger* backend_logger){
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    backend_loggers.push_back(backend_logger);
    backend_logger->SetConnectedToFrontend(true);
  }
}

bool FrontendLogger::RemoveBackendLogger(BackendLogger* _backend_logger){
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    oid_t offset=0;

    for(auto backend_logger : backend_loggers){
      if( backend_logger == _backend_logger){
        backend_logger->SetConnectedToFrontend(false);
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
