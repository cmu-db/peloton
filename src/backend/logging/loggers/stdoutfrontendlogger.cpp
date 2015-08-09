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
 * @brief Collect LogRecords from BackendLogger 
 *  and flush if LogRecordCount() is greater than buffer size
 */
void StdoutFrontendLogger::MainLoop(void) {
  for(int i=0;;i++){
    sleep(5);

    CollectLogRecord();

    if( GetLogRecordCount() >= buffer_size ){
      Flush();
    }
  }
}

void StdoutFrontendLogger::CollectLogRecord(void) {
  backend_loggers = GetBackendLoggers();

  // Look over current frontend logger's backend loggers
  for(auto backend_logger : backend_loggers){
    auto commit_offset = backend_logger->GetCommitOffset();

    // Skip this backend_logger, nothing to do
    if( commit_offset == 0 ) continue; 

    for(oid_t log_record_itr=0; log_record_itr<commit_offset; log_record_itr++){
      // Copy LogRecord from backend_logger to here
      stdout_buffer.push_back(backend_logger->GetLogRecord(log_record_itr));
    }
    backend_logger->Truncate(commit_offset);
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void StdoutFrontendLogger::Flush(void) const {

  std::cout << "\n::StartFlush::\n";

  for( auto record : stdout_buffer ){
    std::cout << record;
  }
  std::cout << "::Commit::" << std::endl;
  stdout_buffer.clear();
}

/**
 * @brief Get buffer size
 * @return return the size of buffer
 */
size_t StdoutFrontendLogger::GetLogRecordCount() const{
  return stdout_buffer.size();
}

}  // namespace logging
}  // namespace peloton
