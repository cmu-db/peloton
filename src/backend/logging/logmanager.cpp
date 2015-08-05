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

#include "backend/logging/logdefs.h"
#include "backend/logging/logmanager.h"
#include "backend/logging/logproxy.h"
#include "backend/logging/aries_proxy.h"
#include "backend/logging/peloton_proxy.h"

namespace peloton {
namespace logging {

LogManager& LogManager::GetInstance(void)
{
  static LogManager logManager;
  return logManager;
}

void LogManager::StartAriesLogging(void){
  auto& logManager = GetInstance();
  auto logger = logManager.GetAriesLogger();
  logger->logging_MainLoop();
}

void LogManager::StartPelotonLogging(void){
  auto& logManager = GetInstance();
  auto logger = logManager.GetPelotonLogger();
  logger->logging_MainLoop();
}

void LogManager::InitAriesLogger(void) {
  AriesProxy* aries_proxy = new AriesProxy();
  aries_logger = new Logger(ARIES_LOGGER_ID, aries_proxy);
}

void LogManager::InitPelotonLogger(void) {
  PelotonProxy* peloton_proxy = new PelotonProxy();
  peloton_logger = new Logger(PELOTON_LOGGER_ID, peloton_proxy);
}

Logger* LogManager::GetAriesLogger(void) {
  if(aries_logger==nullptr) InitAriesLogger();
  return aries_logger;
}

Logger* LogManager::GetPelotonLogger(void) {
  if( peloton_logger==nullptr) InitPelotonLogger();
  return peloton_logger;
}

}  // namespace logging
}  // namespace peloton


