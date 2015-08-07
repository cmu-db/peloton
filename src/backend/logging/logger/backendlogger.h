/*-------------------------------------------------------------------------
 *
 * backendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/backendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logrecord.h"
#include "backend/logging/logger/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Backend Logger 
//===--------------------------------------------------------------------===//

static oid_t logger_id_count=0;

class BackendLogger : public Logger{

  public:
    BackendLogger() { logging_type = LOGGING_TYPE_BACKEND;
                      logger_id = logger_id_count++;}

    static BackendLogger* GetBackendLogger(LoggerType logger_type);

    virtual void log(LogRecord record) = 0;

    oid_t GetBackendLoggerId() const;

  protected:
    oid_t logger_id;
};

}  // namespace logging
}  // namespace peloton
