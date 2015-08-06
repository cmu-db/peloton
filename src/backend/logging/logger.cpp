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
 * @brief Run logging_MainLoop
 */
void Logger::logging_MainLoop(void){
  proxy->logging_MainLoop();
}

/**
 * @brief Run log
 * @param record 
 */
void Logger::log(LogRecord record){
  proxy->log(record);
}

/**
 * @brief Run flush
 */
void Logger::checkpoint(void){
  proxy->flush();
}

}  // namespace logging
}  // namespace peloton


