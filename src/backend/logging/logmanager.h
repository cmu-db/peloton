/*-------------------------------------------------------------------------
 *
 * logmanager.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logmanager.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logger.h"
#include "backend/logging/frontendlogger.h"
#include "backend/logging/backendlogger.h"

#include <memory>
#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Manager
//===--------------------------------------------------------------------===//

static std::mutex logManager_mutex;
static std::mutex frontend_logger_mutex;

/**
 * Global Log Manager
 */
class LogManager{

  public:
    LogManager(const LogManager &) = delete;
    LogManager &operator=(const LogManager &) = delete;
    LogManager(LogManager &&) = delete;
    LogManager &operator=(LogManager &&) = delete;

    // global singleton
    static LogManager& GetInstance(void);

    void StandbyLogging(LoggingType logging_type);

    void StartLogging(void);

    bool IsPelotonReadyToRestore(void);

    BackendLogger* GetBackendLogger(LoggingType logging_type);

  private:

    LogManager(){};

    FrontendLogger* GetFrontendLogger(LoggingType logging_type);

    // frontend_logger is only one for each logging(stdoud, aries, peloton)
    // so that we can identify frontend_logger using logger_type
    std::vector<FrontendLogger*> frontend_loggers;

    bool isPelotonReadyToRestore;

};


}  // namespace logging
}  // namespace peloton
