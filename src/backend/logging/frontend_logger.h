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

#include <iostream>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unistd.h>

#include "backend/common/types.h"
#include "backend/logging/logger.h"
#include "backend/logging/backend_logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Frontend Logger 
//===--------------------------------------------------------------------===//


class FrontendLogger : public Logger{

  public:

    FrontendLogger(){ logger_type = LOGGER_TYPE_FRONTEND; }

    ~FrontendLogger(){
      for(auto backend_logger : backend_loggers){
        delete backend_logger;
      }
    }

    static FrontendLogger* GetFrontendLogger(LoggingType logging_type);

    void MainLoop(void);

    void CollectLogRecord(void);

    void AddBackendLogger(BackendLogger* backend_logger);

    bool RemoveBackendLogger(BackendLogger* backend_logger);

    void NotifyFrontend(bool hasNewLog = false);

    void SetTestInterruptCommit(bool suspend) {
      suspend_committing = suspend;
    }

    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    /**
     * Flush collected LogRecord to stdout or file or nvram
     */
    virtual void Flush(void) = 0;

    /**
     * Restore database
     */
    virtual void DoRecovery(void) = 0;

  protected:

    // Associated backend loggers
    std::vector<BackendLogger*> backend_loggers;
    // Since backend loggers can add themselves into the list above
    // via log manager, we need to protect the backend_loggers list
    std::mutex backend_logger_mutex;

    // Global queue
    std::vector<LogRecord*> global_queue;

    // Used for waking up frontend logger only when backend loggers
    // are ready
    std::mutex backend_notify_mutex;
    std::condition_variable backend_notify_cv;
    uint32 wait_timeout = 5; // in seconds
    bool log_collect_request = false; // used to indicate if backend has new logs

    // suspend final step in transaction commit for testing recovery
    bool suspend_committing = false;
};

}  // namespace logging
}  // namespace peloton
