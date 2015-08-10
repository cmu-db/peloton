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

#include <sys/stat.h>
#include <fcntl.h>

namespace peloton {
namespace logging {

AriesFrontendLogger::AriesFrontendLogger(){
  logging_type = LOGGING_TYPE_STDOUT;

  std::string filename = baseDirectory + "aries_log.txt";

  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT , mode);
  if (fd == -1)
    printf("Something went wrong while creating the logfile: %s\n", strerror(errno));
}

/**
 * @brief MainLoop
 */
void AriesFrontendLogger::MainLoop(void) {
  for(int i=0;;i++){
    sleep(5);

    // Collect LogRecords from BackendLogger 
    CollectLogRecord();

    // If LogRecound count is greater than bufer size,
    if( GetLogRecordCount() >= aries_global_queue_size ){
      // flush the global_queue to aries
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
      aries_global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
    }
    backend_logger->Truncate(commit_offset);
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void AriesFrontendLogger::Flush(void) const {

  std::stringstream log;

  log << "\n::StartFlush::\n";
  for( auto record : aries_global_queue ){
    log << record;
  }
  log << "::Commit::\n";

  //Since frontend logger is only one, we don't need lock anymore
  write(fd, (void*)log.str().c_str(), log.str().length());

  aries_global_queue.clear();
}

/**
 * @brief Get global_queue size
 * @return return the size of global_queue
 */
size_t AriesFrontendLogger::GetLogRecordCount() const{
  return aries_global_queue.size();
}

}  // namespace logging
}  // namespace peloton
