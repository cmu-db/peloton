/*-------------------------------------------------------------------------
 *
 * ariesfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontendlogger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Aries Frontend Logger 
//===--------------------------------------------------------------------===//

class AriesFrontendLogger : public FrontendLogger{
  public:
    void MainLoop(void) const;

    void flush(void) const;
};

}  // namespace logging
}  // namespace peloton
