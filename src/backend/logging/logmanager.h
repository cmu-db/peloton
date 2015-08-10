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

    // global singleton
    static std::shared_ptr<LogManager>& GetInstance(void);

    void StartLogging(LoggingType logging_type);

    void Restore(LoggingType logging_type);
    
    BackendLogger* GetBackendLogger(LoggingType logging_type);

  private:

    // frontend_logger is only one for each logging(stdoud, aries, peloton)
    // so that we can identify frontend_logger using logger_type
    std::vector<FrontendLogger*> frontend_loggers;

};


}  // namespace logging
}  // namespace peloton
