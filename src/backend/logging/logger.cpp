/*-------------------------------------------------------------------------
 *
 * logger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/logger.h"

namespace peloton {
namespace logging {

void Logger::logging_MainLoop(void){
  proxy->logging_MainLoop();
}

void Logger::log(LogRecord record){
  proxy->log(record);
}

}  // namespace logging
}  // namespace peloton


