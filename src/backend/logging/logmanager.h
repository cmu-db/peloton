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

class LogManager{

  public:
    static LogManager& GetInstance(void);

    static void StartAriesLogging(void);

    static void StartPelotonLogging(void);

    Logger* GetAriesLogger(void) ;

    Logger* GetPelotonLogger(void) ;

  private:
    LogManager() {}

    void InitAriesLogger();

    void InitPelotonLogger();

    Logger* aries_logger;

    Logger* peloton_logger;
};

}  // namespace logging
}  // namespace peloton
