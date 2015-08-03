/*-------------------------------------------------------------------------
 *
 * logger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "logger.h"

#include <iostream> // cout
#include <unistd.h> // sleep

namespace peloton {
namespace logging {

void Logger::logging_MainLoop(void){
  for(;;){
      sleep(3);
      std::cout << "logging main loop \n";
  }
}

}  // namespace logging
}  // namespace peloton


