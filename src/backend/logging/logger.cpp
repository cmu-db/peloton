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

/**
 * @brief Run logging_MainLoop to receive log record and flush
 */
void Logger::logging_MainLoop(void){
  proxy->logging_MainLoop();
}

/**
 * @brief Run logging_MainLoop to receive log record and flush
 */
void Logger::log(LogRecord record){
  proxy->log(record);
}

}  // namespace logging
}  // namespace peloton


