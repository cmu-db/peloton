/*-------------------------------------------------------------------------
 *
 * logmanager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logmanager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/logmanager.h"
#include "backend/logging/logproxy.h"
#include "backend/logging/aries_proxy.h"
#include "backend/logging/peloton_proxy.h"

namespace peloton {
namespace logging {

/**
 * @brief Getting the singleton for log manager
 * @return thread local log manager
 */
LogManager& LogManager::GetInstance(void)
{
  static LogManager logManager;
  return logManager;
}

/**
 * @brief Start Aries logging
 * @param buffer size size of the buffer to flush
 */
void LogManager::StartAriesLogging(oid_t buffer_size){
  auto& logManager = GetInstance();
  logManager.InitAriesLogger(buffer_size);
  auto logger = logManager.GetAriesLogger();
  logger->logging_MainLoop();
}

/**
 * @brief Start Peloton logging
 * @param buffer size size of the buffer to flush
 */
void LogManager::StartPelotonLogging(oid_t buffer_size){
  auto& logManager = GetInstance();
  logManager.InitPelotonLogger(buffer_size);
  auto logger = logManager.GetPelotonLogger();
  logger->logging_MainLoop();
}

/**
 * @brief Initialize Aries logger
 * @param buffer size size of the buffer to flush
 */
void LogManager::InitAriesLogger(oid_t buffer_size) {
  AriesProxy* aries_proxy = new AriesProxy(buffer_size);
  aries_logger = new Logger(ARIES_LOGGER_ID, aries_proxy);
}

/**
 * @brief Initialize Peloton logger
 * @param buffer size size of the buffer to flush
 */
void LogManager::InitPelotonLogger(oid_t buffer_size) {
  PelotonProxy* peloton_proxy = new PelotonProxy(buffer_size);
  peloton_logger = new Logger(PELOTON_LOGGER_ID, peloton_proxy);
}

/**
 * @brief Get the Aries logger
 *  NOTE :: buffer size is only needed for logger that runs logging_MainLoop
 *  otherwise, logger will be initialized without buffer size and it will
 *  be used for log record
 * @return aries logger
 */
Logger* LogManager::GetAriesLogger(void) {
  if(aries_logger==nullptr) InitAriesLogger();
  return aries_logger;
}

/**
 * @brief Get the Peloton logger
 *  NOTE :: buffer size is only needed for logger that runs logging_MainLoop
 *  otherwise, logger will be initialized without buffer size and it will
 *  be used for log record
 * @return peloton logger
 */
Logger* LogManager::GetPelotonLogger(void) {
  if( peloton_logger==nullptr) InitPelotonLogger();
  return peloton_logger;
}

}  // namespace logging
}  // namespace peloton


