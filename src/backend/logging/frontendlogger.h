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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Frontend Logger 
//===--------------------------------------------------------------------===//

static std::mutex backend_logger_mutex;

class FrontendLogger : public Logger{

  public:

    FrontendLogger(){ logger_type=LOGGER_TYPE_FRONTEND; }

    static FrontendLogger* GetFrontendLogger(LoggingType logging_type);

    void AddBackendLogger(BackendLogger* backend_logger);

    std::vector<BackendLogger*> GetBackendLoggers(void);


    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    virtual void MainLoop(void) const = 0;

    virtual void commit(void) const = 0;

    virtual void CollectLogRecords(void) = 0;

    virtual size_t GetLogCount(void) const = 0;

  private:

    std::vector<BackendLogger*> backend_loggers;

};

}  // namespace logging
}  // namespace peloton
