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

  virtual void Print(void) = 0;

  char* GetMessage(void) const {return message;}

  size_t GetMessageLength(void) const {return message_length;}

 protected:

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  // serialized message
  char* message = nullptr;

  // length of the message
  size_t message_length = 0;

};

}  // namespace logging
}  // namespace peloton
