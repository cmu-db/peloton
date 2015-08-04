/*-------------------------------------------------------------------------
 *
 * logproxy.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logproxy.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logrecord.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Proxy
//===--------------------------------------------------------------------===//


class LogProxy{

  public:

    virtual void log(LogRecord record) const = 0;

    virtual void logging_MainLoop(void) const = 0;

    virtual ~LogProxy() {}
 
};

}  // namespace logging
}  // namespace peloton
