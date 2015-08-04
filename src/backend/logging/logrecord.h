/*-------------------------------------------------------------------------
 *
 * logrecord.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logrecord.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"

#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord{

public:
  LogRecord() = delete;
  LogRecord(LogType log_type, oid_t database_oid) 
  : log_type(log_type), database_oid(database_oid) {} ;

  oid_t GetDbOid() const;

  LogType GetType() const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:
  LogType log_type = LOG_TYPE_INVALID;
  oid_t database_oid = INVALID_OID;

};

}  // namespace logging
}  // namespace peloton
