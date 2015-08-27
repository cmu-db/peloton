/*-------------------------------------------------------------------------
 *
 * stdoutbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/backend_logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Stdout Backend Logger 
//===--------------------------------------------------------------------===//

class StdoutBackendLogger : public BackendLogger{

  public:

    static StdoutBackendLogger* GetInstance(void);

    void Insert(LogRecord* record);

    void Delete(LogRecord* record);

    void Update(LogRecord* record);

  private:

    StdoutBackendLogger(){ logging_type = LOGGING_TYPE_STDOUT;}

};

}  // namespace logging
}  // namespace peloton
