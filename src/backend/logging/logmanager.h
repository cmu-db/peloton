/*-------------------------------------------------------------------------
 *
 * logmanager.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logmanager.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Manager
//===--------------------------------------------------------------------===//

/**
 * A LogManager contains a hard coded set of loggers that have counterpart loggers elsewhere.
 */
class LogManager{

  public:
    static LogManager& GetInstance(void);

    static void StartAriesLogging(oid_t buffer_size);

    static void StartPelotonLogging(oid_t buffer_size);

    Logger* GetAriesLogger(void) ;

    Logger* GetPelotonLogger(void) ;

  private:
    LogManager() {}

    // Default buffer size is 0, which means logger is 'LOG ONLY'
    void InitAriesLogger(oid_t buffer_size = 0 /* LOG ONLY */);
    void InitPelotonLogger(oid_t buffer_size = 0 /*LOG ONLY*/);

    Logger* aries_logger;

    Logger* peloton_logger;
};

}  // namespace logging
}  // namespace peloton
