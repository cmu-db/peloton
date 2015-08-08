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
#include "backend/logging/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Backend Logger 
//===--------------------------------------------------------------------===//

static oid_t backend_id_count=0;

class BackendLogger : public Logger{

  public:
    BackendLogger() { logger_type = LOGGER_TYPE_BACKEND;
                      backend_id = backend_id_count++;}

    static BackendLogger* GetBackendLogger(LoggingType logging_type);

    virtual void log(LogRecord record) = 0;

    virtual void flush(void) = 0;

    oid_t GetBackendLoggerId() const;

  protected:
    oid_t backend_id;
};

}  // namespace logging
}  // namespace peloton
