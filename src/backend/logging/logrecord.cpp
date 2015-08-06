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

LogRecordType LogRecord::GetType() const{
  return log_record_type;
}

oid_t LogRecord::GetDbOid() const{
  return database_oid;
}

const concurrency::Transaction* LogRecord::GetTxn() const{
  return transaction;
}

char* LogRecord::GetData() const{
  return serialized_data;
}

size_t LogRecord::GetDataSize() const{
  return serialized_data_size;
}

std::ostream& operator<<(std::ostream& os, const LogRecord& record) {
  os << "LOG TYPE (" << record.GetType() << ")\n";
  os << "DB OID (" << record.GetDbOid() << ") \n";
  os << "GetData (" << record.GetData() << ") \n";

  return os;
}


}  // namespace logging
}  // namespace peloton
