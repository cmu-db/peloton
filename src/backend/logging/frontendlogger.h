/*-------------------------------------------------------------------------
 *
 * frontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/frontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/logging/logger.h"
#include "backend/logging/backendlogger.h"

#include <iostream>
#include <mutex>
#include <vector>
#include <unistd.h>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Frontend Logger 
//===--------------------------------------------------------------------===//


class FrontendLogger : public Logger{

  public:

    FrontendLogger(){ logger_type=LOGGER_TYPE_FRONTEND; }

    static FrontendLogger* GetFrontendLogger(LoggingType logging_type);

    void AddBackendLogger(BackendLogger* backend_logger);

    bool RemoveBackendLogger(BackendLogger* backend_logger);

    std::vector<BackendLogger*> GetBackendLoggers(void);

    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    /**
     * Check whether any BackendLogger has been committed or not
     */
    virtual void MainLoop(void) = 0;

    /**
     * Collect LogRecord from BackendLoggers
     */
    virtual void CollectLogRecord(void) = 0;

    /**
     * Flush collected LogRecord to stdout or file or nvram
     */
    virtual void Flush(void) = 0;

    /**
     * Restore database
     */
    virtual void Recovery(void) = 0;

  protected:

    std::vector<BackendLogger*> backend_loggers;

    std::mutex backend_logger_mutex;

};

}  // namespace logging
}  // namespace peloton
