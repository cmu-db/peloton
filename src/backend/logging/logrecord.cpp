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

size_t LogRecord::GetSerializedLogRecordHeaderSize() {
  return sizeof(char)+sizeof(int16_t)+sizeof(int16_t)+sizeof(int64_t);;
}

/**
 * @brief Serialize LogRecordHeader
 * @param output  
 */
void LogRecord::SerializeLogRecordHeader(CopySerializeOutput& output){
  output.WriteEnumInSingleByte(log_record_type);
  output.WriteShort(db_oid);
  output.WriteShort(table_oid);
  output.WriteLong(txn_id);
}

/**
 * @brief Serialize given data
 * @param data 
 * @return true if we serialize data otherwise false
 */
bool LogRecord::SerializeLogRecord(const void* data){
  bool status = true;

  CopySerializeOutput output;
  SerializeLogRecordHeader(output);

  switch(log_record_type){
    case LOGRECORD_TYPE_INSERT_TUPLE:{
     storage::Tuple* tuple = (storage::Tuple*)data;
     tuple->SerializeTo(output);
    } break;

    default:{
      LOG_WARN("Unsupported LOG TYPE\n");
      status = false;
    } break;
  }

  serialized_log_record_size = output.Size();
  serialized_log_record = (char*)malloc(serialized_log_record_size);
  memcpy( serialized_log_record, output.Data(), serialized_log_record_size);

  return status;
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input  
 */
void LogRecord::DeserializeLogRecordHeader(CopySerializeInput& input){
  log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  db_oid = (oid_t)(input.ReadShort());
  table_oid = (oid_t)(input.ReadShort());
  txn_id = (txn_id_t)(input.ReadLong());
}

/**
 * @brief Deserialize from given data
 * @param data 
 * @return true if we deserialize data otherwise false
 */
bool LogRecord::DeserializeLogRecord(const void* data, size_t data_size){
  bool status = true;

  CopySerializeInput input(data, data_size);
  DeserializeLogRecordHeader(input);

  std::cout << "log type  : " << LogRecordTypeToString(log_record_type) << std::endl;
  std::cout << "db oid : " << db_oid << std::endl;
  std::cout << "table oid : " << table_oid << std::endl;
  std::cout << "txn id : " << txn_id << std::endl;
 
 //TODO :: 
//  switch(log_record_type){
//    case LOGRECORD_TYPE_INSERT_TUPLE:{
//
//     // get the tuple and tuple info
//     storage::Tuple* tuple = (storage::Tuple*)data;
//     CopySerializeOutput output;
//     tuple->SerializeTo(output);
//     
//     // setting serializd_data and size
//     // TODO :: serialize database oid, table_oid, txn_id, type and put them here together
//     serialized_data = output.Data();
//     serialized_data_size = output.Size();
//
//    } break;
//
//    default:{
//      LOG_WARN("Unsupported LOG TYPE\n");
//      status = false;
//    } break;
//  }
  return status;
}

char* LogRecord::GetSerializedLogRecord() const{
  return serialized_log_record;
}

size_t LogRecord::GetSerializedLogRecordSize() const{
  return serialized_log_record_size;
}

LogRecordType LogRecord::GetType() const{
  return log_record_type;
}

oid_t LogRecord::GetDbId() const{
  return db_oid;
}

oid_t LogRecord::GetTableId() const{
  return table_oid;
}

txn_id_t LogRecord::GetTxnId() const{
  return txn_id;
}

ItemPointer LogRecord::GetItemPointer() const{
  return itemPointer;
}

std::ostream& operator<<(std::ostream& os, const LogRecord& record) {
  os << "#LOG TYPE:" << LogRecordTypeToString(record.GetType());
  os << " #Db  ID:" << record.GetDbId();
  os << " #Tb  ID:" << record.GetTableId();
  os << " #Txn ID:" << record.GetTxnId();
  os << " #Location :" << record.GetItemPointer().block;
  os << " " << record.GetItemPointer().offset;
  os << "\n";
  return os;
}

}  // namespace logging
}  // namespace peloton
