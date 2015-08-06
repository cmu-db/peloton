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


/**
 * @brief Serialize given data
 * @param data 
 * @return true if we serialize data otherwise false
 */
bool LogRecord::SerializeData(const void* data){
  bool status = true;

  switch(log_record_type){
    case LOGRECORD_TYPE_INSERT_TUPLE:{
     // get the tuple and tuple info
     storage::Tuple* tuple = (storage::Tuple*)data;
     std::string tuple_info = tuple->GetInfo();

     // setting serializd_data and size
     serialized_data_size = tuple_info.size();
     serialized_data = (char*)malloc(serialized_data_size);

     for(int str_itr=0; str_itr<tuple_info.size(); str_itr++){
       serialized_data[str_itr] = tuple_info[str_itr];
     }
    } break;

    default:{
      LOG_WARN("Unsupported LOG TYPE\n");
      status = false;
    } break;
  }

  return status;
}

LogRecordType LogRecord::GetType() const{
  return log_record_type;
}

oid_t LogRecord::GetDbOid() const{
  return database_oid;
}

const concurrency::Transaction* LogRecord::GetTxn() const{
  return transaction;
}

const char* LogRecord::GetData() const{
  return serialized_data;
}

size_t LogRecord::GetDataSize() const{
  return serialized_data_size;
}

std::ostream& operator<<(std::ostream& os, const LogRecord& record) {
  os << "#LOG TYPE:" << LogRecordTypeToString(record.GetType());
  os << "#DB OID:" << record.GetDbOid();
  os << "#Txn ID:" << record.GetTxn()->GetTransactionId();
  os << "#Serialized Data:" << record.GetData();

  return os;
}


}  // namespace logging
}  // namespace peloton
