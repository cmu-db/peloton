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

#include "backend/logging/records/tuplerecord.h"

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

  // serialize other parts depends on type
  switch(GetType()){

    case LOGRECORD_TYPE_TUPLE_INSERT:
    case LOGRECORD_TYPE_TUPLE_UPDATE:{
     storage::Tuple* tuple = (storage::Tuple*)data;
     tuple->SerializeTo(output);
    }break;

    case LOGRECORD_TYPE_TUPLE_DELETE:{
    // nothing to do now 
    }break;

    default:{
      LOG_WARN("Unsupported TUPLE RECORD TYPE\n");
      status = false;
    }break;
  }

  serialized_data_size = output.Size();
  serialized_data = (char*)malloc(serialized_data_size);
  memset( serialized_data, 0, serialized_data_size);
  memcpy( serialized_data, output.Data(), serialized_data_size);

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
  output.WriteShort(itemPointer.block);
  output.WriteShort(itemPointer.offset);

  output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input  
 */
void TupleRecord::DeserializeHeader(CopySerializeInput& input){
  input.ReadInt();
  db_oid = (oid_t)(input.ReadShort());
  table_oid = (oid_t)(input.ReadShort());
  txn_id = (txn_id_t)(input.ReadLong());
  itemPointer.block = (oid_t)(input.ReadShort());
  itemPointer.offset = (oid_t)(input.ReadShort());
}

std::ostream& operator<<(std::ostream& os, const TupleRecord& tupleRecord) {
  os << "#LOG TYPE:" << LogRecordTypeToString(tupleRecord.GetType()) << "\n";
  os << " #Db  ID:" << tupleRecord.GetDbId() << "\n";
  os << " #Tb  ID:" << tupleRecord.GetTableId() << "\n";
  os << " #Txn ID:" << tupleRecord.GetTxnId() << "\n";
  os << " #Location :" << tupleRecord.GetItemPointer().block;
  os << " " << tupleRecord.GetItemPointer().offset << "\n";
  os << "\n";
  return os;
}

}  // namespace logging
}  // namespace peloton
