/*-------------------------------------------------------------------------
 *
 * ariesfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/ariesfrontendlogger.h"
#include "backend/logging/loggers/ariesbackendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief MainLoop
 */
void AriesFrontendLogger::MainLoop(void) {
  for(int i=0;;i++){
    sleep(5);

    // Collect LogRecords from BackendLogger 
    CollectLogRecord();

    // If LogRecound count is greater than bufer size,
    if( GetLogRecordCount() >= buffer_size ){
      // flush the buffer to aries
      Flush();
    }
  }
}

/**
 * @brief Collect the LogRecord from BackendLogger
 */
void AriesFrontendLogger::CollectLogRecord(void) {
  backend_loggers = GetBackendLoggers();

  // Look over current frontend logger's backend loggers
  for( auto backend_logger : backend_loggers){
    auto commit_offset = ((AriesBackendLogger*)backend_logger)->GetCommitOffset();

    // Skip this backend_logger, nothing to do
    if( commit_offset == 0 ) continue; 

    for(oid_t log_record_itr=0; log_record_itr<commit_offset; log_record_itr++){
      // Copy LogRecord from backend_logger to here
      aries_buffer.push_back(backend_logger->GetLogRecord(log_record_itr));
    }
    backend_logger->Truncate(commit_offset);
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void AriesFrontendLogger::Flush(void) const {

  std::cout << "\n::StartFlush::\n";

  for( auto record : aries_buffer ){
    std::cout << record;
  }
  std::cout << "::Commit::" << std::endl;
  aries_buffer.clear();
}

/**
 * @brief Get buffer size
 * @return return the size of buffer
 */
size_t AriesFrontendLogger::GetLogRecordCount() const{
  return aries_buffer.size();
}

}  // namespace logging
}  // namespace peloton
