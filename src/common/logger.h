/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "easylogging/easylogging++.h"

// Disable logging in production
#ifdef NDEBUG
#define ELPP_DISABLE_LOGS
#endif

#define _ELPP_NO_DEFAULT_LOG_FILE

namespace nstore {

class Logger {

 public:
  Logger() {
    // Load configuration from file
    el::Configurations conf("./log.conf");

    // Reconfigure default logger
    el::Loggers::reconfigureLogger("default", conf);
  }

};

} // End nstore namespace

