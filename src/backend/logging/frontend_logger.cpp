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
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {

  auto& logManager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Standby Mode");
  // Standby before we are ready to recovery
  while(logManager.GetStatus(LOGGING_TYPE_PELOTON) == LOGGING_STATUS_TYPE_STANDBY ){
    sleep(1);
  }

  // Do recovery if we can, otherwise terminate
  switch(logManager.GetStatus(LOGGING_TYPE_PELOTON)){
    case LOGGING_STATUS_TYPE_RECOVERY:{
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      DoRecovery();

      // Now, enable active logging
      logManager.SetLoggingStatus(LOGGING_TYPE_PELOTON, LOGGING_STATUS_TYPE_LOGGING);

      break;
    }

    case LOGGING_STATUS_TYPE_LOGGING:{
      LOG_TRACE("Frontendlogger] Ongoing Mode");
    }
    break;

    default:
    break;
  }

  /////////////////////////////////////////////////////////////////////
  // LOGGING MODE
  /////////////////////////////////////////////////////////////////////

  // Periodically, wake up and do logging
  while(logManager.GetStatus(LOGGING_TYPE_PELOTON) == LOGGING_STATUS_TYPE_LOGGING){
    sleep(1);

    // Collect LogRecords from all backend loggers
    CollectLogRecord();

    // Flush the data to the file
    Flush();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // flush any remaining log records
  CollectLogRecord();
  Flush();


  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Sleep Mode");

  //Setting frontend logger status to sleep
  logManager.SetLoggingStatus(LOGGING_TYPE_PELOTON, 
                              LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the LogRecord from BackendLoggers
 */
void FrontendLogger::CollectLogRecord() {

  backend_loggers = GetBackendLoggers();

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
