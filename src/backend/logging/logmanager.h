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

#include "backend/common/types.h"
#include "backend/logging/logger/logger.h"
#include "backend/logging/logger/frontendlogger.h"
#include "backend/logging/logger/backendlogger.h"

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
static std::mutex backend_logger_mutex;

/**
 * Global Log Manager
 */
class LogManager{
  public:
    static std::shared_ptr<LogManager>& GetInstance(void);
    
  private:
    void StartLogging(LoggerType logger_type);

    BackendLogger* GetBackendLogger(LoggerType logger_type);
 
    // frontend logger is only one for each stdoud, aries, and peloton
    std::vector<FrontendLogger*> frontend_loggers;

    std::vector<BackendLogger*> backend_loggers;

};

}  // namespace logging
}  // namespace peloton
