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
#include "backend/bridge/ddl/bridge.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord{

public:
  LogRecord( LogRecordType log_record_type) 
  : log_record_type(log_record_type) { 
  assert(log_record_type != LOGRECORD_TYPE_INVALID);
  }

  virtual ~LogRecord(){}

  LogRecordType GetType() const{ return log_record_type; }

  virtual bool Serialize(void) = 0;

  virtual void print(void) = 0;

  char* GetSerializedData(void) const {return serialized_data;}

  size_t GetSerializedDataSize(void) const {return serialized_data_size;}

protected:

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  char* serialized_data;

  size_t serialized_data_size = 0;

};

}  // namespace logging
}  // namespace peloton
