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

#include "backend/common/logger.h"
#include "backend/logging/logrecord.h"
#include "backend/storage/tuple.h"

#include <iostream>

namespace peloton {
namespace logging {

/**
 * @brief Serialize given data
 * @param data 
 * @return true if we serialize data otherwise false
 */
bool LogRecord::SerializeLogRecord(){
  CopySerializeOutput output;
  bool status = true;

  // Serialize the header of LogRecord
  log_record_header.SerializeLogRecordHeader(output);

  switch(log_record_header.GetType()){

    case LOGRECORD_TYPE_TUPLE_INSERT:
    case LOGRECORD_TYPE_TUPLE_UPDATE:{
     storage::Tuple* tuple = (storage::Tuple*)data;
     tuple->SerializeTo(output);
    }break;

    case LOGRECORD_TYPE_TUPLE_DELETE:{
    }break;

    default:{
      LOG_WARN("Unsupported LOG TYPE\n");
      status = false;
    }break;
  }

  serialized_log_record_size = output.Size();
  serialized_log_record = (char*)malloc(serialized_log_record_size);
  memcpy( serialized_log_record, output.Data(), serialized_log_record_size);

  return status;
}

LogRecordHeader LogRecord::GetHeader() const{
  return log_record_header;
}

char* LogRecord::GetSerializedLogRecord() const{
  return serialized_log_record;
}

size_t LogRecord::GetSerializedLogRecordSize() const{
  return serialized_log_record_size;
}

std::ostream& operator<<(std::ostream& os, const LogRecord& record) {

  os << record.GetHeader();
  return os;

}

}  // namespace logging
}  // namespace peloton
