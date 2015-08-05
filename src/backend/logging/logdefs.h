/*-------------------------------------------------------------------------
 *
 * logdefs.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logdefs.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Definitions
//===--------------------------------------------------------------------===//

enum LoggerId{
  INVALID_LOGGER_ID = 0, 

  ARIES_LOGGER_ID   = 1, 
  PELOTON_LOGGER_ID = 2
};


}  // namespace logging
}  // namespace peloton


