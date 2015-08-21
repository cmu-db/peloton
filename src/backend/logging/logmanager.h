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

#include <mutex>
#include <map>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Manager
//===--------------------------------------------------------------------===//

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

    void SetMainLoggingType(LoggingType logging_type);

    LoggingType GetMainLoggingType(void);

    void StandbyLogging(LoggingType logging_type = LOGGING_TYPE_INVALID );

    void StartLogging(LoggingType logging_type = LOGGING_TYPE_INVALID);

    bool EndLogging(LoggingType logging_type =  LOGGING_TYPE_INVALID);

    bool IsReadyToLogging(LoggingType logging_type = LOGGING_TYPE_INVALID);

    size_t ActiveFrontendLoggerCount(void) const;

    void SetLoggingStatus(LoggingType logging_type, LoggingStatus logging_status);

    void MakeItSleepy(LoggingType logging_type = LOGGING_TYPE_INVALID);

    LoggingStatus GetLoggingStatus(LoggingType logging_type = LOGGING_TYPE_INVALID);

    BackendLogger* GetBackendLogger(LoggingType logging_type = LOGGING_TYPE_INVALID);

  private:

    LogManager(){};

    FrontendLogger* GetFrontendLogger(LoggingType logging_type);

    std::mutex frontend_logger_mutex;

    std::mutex logging_status_mutex;

    LoggingType MainLoggingType = LOGGING_TYPE_INVALID;

    // frontend_logger is only one for each logging(stdoud, aries, peloton)
    // so that we can identify frontend_logger using logger_type
    std::vector<FrontendLogger*> frontend_loggers;

    std::map<LoggingType, LoggingStatus> logging_statuses;
};


}  // namespace logging
}  // namespace peloton
