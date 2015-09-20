/*-------------------------------------------------------------------------
 *
 * tuplerecord.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/records/tuplerecord.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/records/tuple_record.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace logging {

/**
 * @brief Serialize given data
 * @return true if we serialize data otherwise false
 */
bool TupleRecord::Serialize(){

  bool status = true;
  CopySerializeOutput output;

  // Serialize the common variables such as database oid, table oid, etc.
  SerializeHeader(output);

  // Serialize other parts depends on type
  switch(GetType()){

    case LOGRECORD_TYPE_ARIES_TUPLE_INSERT:
    case LOGRECORD_TYPE_ARIES_TUPLE_UPDATE:  {
      storage::Tuple* tuple = (storage::Tuple*)data;
      tuple->SerializeTo(output);
      break;
    }

    case LOGRECORD_TYPE_ARIES_TUPLE_DELETE:
      // Nothing to do here !
      break;

    case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:  
    case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:  
    case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
      // Nothing to do here !
      break;

    default:  {
      LOG_WARN("Unsupported TUPLE RECORD TYPE\n");
      status = false;
      break;
    }
  }

  message_length = output.Size();
  message = (char*)malloc(message_length);
  memset( message, 0, message_length);
  memcpy( message, output.Data(), message_length);

  return status;
}

/**
 * @brief Serialize LogRecordHeader
 * @param output  
 */
void TupleRecord::SerializeHeader(CopySerializeOutput& output){

  // Record LogRecordType first 
  output.WriteEnumInSingleByte(log_record_type);

  size_t start = output.Position();
  // then reserve 4 bytes for the header size
  output.WriteInt(0);

  output.WriteShort(db_oid);
  output.WriteShort(table_oid);
  output.WriteLong(txn_id);
  output.WriteShort(insert_location.block);
  output.WriteShort(insert_location.offset);
  output.WriteShort(delete_location.block);
  output.WriteShort(delete_location.offset);

  output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input  
 */
void TupleRecord::DeserializeHeader(CopySerializeInputBE& input) {
  input.ReadInt();
  db_oid = (oid_t)(input.ReadShort());
  assert(db_oid);
  table_oid = (oid_t)(input.ReadShort());
  assert(table_oid);
  txn_id = (txn_id_t)(input.ReadLong());
  assert(txn_id);
  insert_location.block = (oid_t)(input.ReadShort());
  insert_location.offset = (oid_t)(input.ReadShort());
  delete_location.block = (oid_t)(input.ReadShort());
  delete_location.offset = (oid_t)(input.ReadShort());
}

// Used for peloton logging
size_t TupleRecord::GetTupleRecordSize(void){
  // log_record_type + header_legnth + db_oid + table_oid + txn_id + insert_location + delete_location 
  return sizeof(char) + sizeof(int) + sizeof(int16_t) + sizeof(int16_t) + sizeof(long) + sizeof(int16_t)*4;
}

//just for debugging
void TupleRecord::Print(){
  std::cout << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
  std::cout << " #Db  ID:" << GetDatabaseOid() << "\n";
  std::cout << " #Tb  ID:" << GetTableId() << "\n";
  std::cout << " #Txn ID:" << GetTransactionId() << "\n";
  std::cout << " #Insert Location :" << GetInsertLocation().block;
  std::cout << " " << GetInsertLocation().offset << "\n";
  std::cout << " #Delete Location :" << GetDeleteLocation().block;
  std::cout << " " << GetDeleteLocation().offset << "\n";
  std::cout << "\n";
}

}  // namespace logging
}  // namespace peloton
