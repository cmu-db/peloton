/*-------------------------------------------------------------------------
 *
 * stdoutfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/stdoutfrontendlogger.h"
#include "backend/logging/loggers/stdoutbackendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief MainLoop
 */
void StdoutFrontendLogger::MainLoop(void) {
  for(int i=0;;i++){
    sleep(1);

    // Collect LogRecords from BackendLogger 
    CollectLogRecord();

    // Checkpoint ?
    if( GetLogRecordCount() >= stdout_global_queue_size ){
      // flush the global_queue to stdout
      Flush();
    }
  }
}

/**
 * @brief Collect the LogRecord from BackendLogger
 */
void StdoutFrontendLogger::CollectLogRecord() {
  backend_loggers = GetBackendLoggers();

  // Look over current frontend logger's backend loggers
  for( auto backend_logger : backend_loggers){
    auto commit_offset = ((StdoutBackendLogger*)backend_logger)->GetCommitOffset();

    // Skip this backend_logger, nothing to do
    if(commit_offset == 0 ) continue; 

    for(oid_t log_record_itr=0; log_record_itr<commit_offset; log_record_itr++){
      // Copy LogRecord from backend_logger to here
      stdout_global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
    }
    backend_logger->Truncate(commit_offset);
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void StdoutFrontendLogger::Flush(void) {

  std::cout << "\n::StartFlush::\n";

  //for( auto record : stdout_global_queue ){
    //FIXME
    //std::cout << record;
  //}
  std::cout << "::EndFlush::" << std::endl;
  stdout_global_queue.clear();
}

/**
 * @brief Get global_queue size
 * @return return the size of global_queue
 */
size_t StdoutFrontendLogger::GetLogRecordCount() const{
  return stdout_global_queue.size();
}

}  // namespace logging
}  // namespace peloton
