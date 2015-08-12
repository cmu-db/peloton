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
#include "backend/common/serializer.h"
#include "backend/logging/logrecordheader.h"

#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord{

public:

  // INSERT/UPDATE TUPLE
  LogRecord(LogRecordHeader log_record_header, const void* data) 
  : log_record_header(log_record_header), data(data) { }

  // DELETE_TUPLE
  LogRecord(LogRecordHeader log_record_header) 
  : log_record_header(log_record_header){data = nullptr;}

  //===--------------------------------------------------------------------===//
  // Serialization 
  //===--------------------------------------------------------------------===//

  bool SerializeLogRecord();

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  LogRecordHeader GetHeader() const;

  char* GetSerializedLogRecord(void) const;

  size_t GetSerializedLogRecordSize(void) const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  LogRecordHeader log_record_header;

  const void* data;

  char* serialized_log_record;

  size_t serialized_log_record_size;

};

}  // namespace logging
}  // namespace peloton
