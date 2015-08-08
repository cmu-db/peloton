/*-------------------------------------------------------------------------
 *
 * stdoutbackendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutbackendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/stdoutbackendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief Recording log record
 * @param log record 
 */
void StdoutBackendLogger::log(LogRecord record){
  stdout_buffer.push_back(record);
}

/**
 * @brief Flushing the local queue to frontend logger
 */
void StdoutBackendLogger::flush(){
  //stdout_buffer.push_back(record);
  //TODO do something here
}

}  // namespace logging
}  // namespace peloton
