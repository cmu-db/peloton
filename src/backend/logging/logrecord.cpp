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
#include "backend/storage/tuple.h"

#include <iostream>

namespace peloton {
namespace logging {

LogRecordType LogRecord::GetType() const{
  return log_record_type;
}

txn_id_t LogRecord::GetTxnId() const{
  return txn_id;
}

ItemPointer LogRecord::GetItemPointer() const{
  return itemPointer;
}

std::ostream& operator<<(std::ostream& os, const LogRecord& record) {
  os << "#LOG TYPE:" << LogRecordTypeToString(record.GetType());
  os << " #Txn ID:" << record.GetTxnId();
  os << " #Location :" << record.GetItemPointer().block;
  os << " " << record.GetItemPointer().offset;
  os << "\n";

  return os;
}


}  // namespace logging
}  // namespace peloton
