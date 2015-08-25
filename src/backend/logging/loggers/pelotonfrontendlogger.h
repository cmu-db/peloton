/*-------------------------------------------------------------------------
 *
 * pelotonfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontendlogger.h"

namespace peloton {
namespace logging {

static std::vector<LogRecord*> aries_global_queue;

//===--------------------------------------------------------------------===//
// Peloton Frontend Logger 
//===--------------------------------------------------------------------===//

class PelotonFrontendLogger : public FrontendLogger{

  public:

    AriesFrontendLogger(void);

   ~AriesFrontendLogger(void);

    void MainLoop(void);

    void CollectLogRecord(void);

    void Flush(void);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void Recovery(void);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // FIXME :: Hard coded file name
    std::string filename = "peloton.log";

    // File pointer and descriptor
    FILE* logFile;

    int logFileFd;

    // Txn table
    std::map<txn_id_t, concurrency::Transaction *> recovery_txn_table;

    // Keep tracking max oid for setting next_oid in manager 
    oid_t max_oid = INVALID_OID;
}
};

}  // namespace logging
}  // namespace peloton
