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

#include <sstream>

namespace peloton {
namespace logging {

static std::vector<LogRecord> aries_global_queue;

//===--------------------------------------------------------------------===//
// Aries Frontend Logger 
//===--------------------------------------------------------------------===//

class AriesFrontendLogger : public FrontendLogger{

  public:

    AriesFrontendLogger(void);

    void MainLoop(void);

    void CollectLogRecord(void);

    void Flush(void) const;

  private:

    size_t GetLogRecordCount(void) const;

    // FIXME :: Hard coded based dir
    std::string baseDirectory = "/home/parallels/git/peloton/build/";

    // FIXME :: Hard coded global_queue size
    oid_t aries_global_queue_size = 3;

    int fd;

};

}  // namespace logging
}  // namespace peloton
