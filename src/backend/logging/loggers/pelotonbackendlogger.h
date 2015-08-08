/*-------------------------------------------------------------------------
 *
 * pelotonbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/backendlogger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Peloton Backend Logger 
//===--------------------------------------------------------------------===//

class PelotonBackendLogger : public BackendLogger{
  public:
    void log(LogRecord record) const;
};

}  // namespace logging
}  // namespace peloton
