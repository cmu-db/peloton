/*-------------------------------------------------------------------------
 *
 * logrecord.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logrecord.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/logrecord.h"

#include <iostream>

namespace peloton {
namespace logging {

LogType LogRecord::GetType() const {
  return log_type;
}

oid_t LogRecord::GetDbOid() const {
  return database_oid;
}

std::ostream& operator<<(std::ostream& os, const LogRecord& record) {
  os << "LOG TYPE (" << record.GetType() << ")\n";
  os << "DB OID (" << record.GetDbOid() << ") \n";

  return os;
}


}  // namespace logging
}  // namespace peloton
