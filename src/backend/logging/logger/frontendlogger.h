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
#include "backend/logging/logger/logger.h"

#include <vector>
#include <mutex>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Frontend Logger 
//===--------------------------------------------------------------------===//

class FrontendLogger : public Logger{

  public:
    FrontendLogger(){ logging_type=LOGGING_TYPE_FRONTEND; }

    static FrontendLogger* GetFrontendLogger(LoggerType logger_type);

    virtual void MainLoop(void) const = 0;

    virtual void flush(void) const = 0;
};

}  // namespace logging
}  // namespace peloton
