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

#include "backend/logging/logger/stdoutbackendlogger.h"

namespace peloton {
namespace logging {

/**
 * @brief Recording log record
 * @param log record 
 */
void StdoutBackendLogger::log(LogRecord record){
  stdout_buffer.push_back(record);
}

}  // namespace logging
}  // namespace peloton
