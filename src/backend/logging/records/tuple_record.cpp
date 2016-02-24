/*-------------------------------------------------------------------------
 *
 * tuple_record.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/records/tuple_record.cpp
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
bool TupleRecord::Serialize(CopySerializeOutput &output) {
  bool status = true;
  output.Reset();

  // Serialize the common variables such as database oid, table oid, etc.
  SerializeHeader(output);

  // Serialize other parts depends on type
  switch (GetType()) {
    case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
    case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
      storage::Tuple *tuple = (storage::Tuple *)data;
      tuple->SerializeTo(output);
      break;
    }

    case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
      // Nothing to do here !
      break;

    case LOGRECORD_TYPE_WBL_TUPLE_INSERT:
    case LOGRECORD_TYPE_WBL_TUPLE_DELETE:
    case LOGRECORD_TYPE_WBL_TUPLE_UPDATE:
      // Nothing to do here !
      break;

    default: {
      LOG_WARN("Unsupported TUPLE RECORD TYPE");
      status = false;
      break;
    }
  }

  message_length = output.Size();
  message = new char[message_length];
  std::memcpy(message, output.Data(), message_length);

  return status;
}

/**
 * @brief Serialize LogRecordHeader
 * @param output
 */
void TupleRecord::SerializeHeader(CopySerializeOutput &output) {
  // Record LogRecordType first
  output.WriteEnumInSingleByte(log_record_type);

  size_t start = output.Position();
  // then reserve 4 bytes for the header size
  output.WriteInt(0);

  output.WriteLong(db_oid);
  output.WriteLong(table_oid);
  output.WriteLong(txn_id);
  output.WriteLong(insert_location.block);
  output.WriteLong(insert_location.offset);
  output.WriteLong(delete_location.block);
  output.WriteLong(delete_location.offset);

  output.WriteIntAt(
      start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input
 */
void TupleRecord::DeserializeHeader(CopySerializeInputBE &input) {
  input.ReadInt();
  db_oid = (oid_t)(input.ReadLong());
  assert(db_oid);
  table_oid = (oid_t)(input.ReadLong());
  assert(table_oid);
  txn_id = (txn_id_t)(input.ReadLong());
  assert(txn_id);
  insert_location.block = (oid_t)(input.ReadLong());
  insert_location.offset = (oid_t)(input.ReadLong());
  delete_location.block = (oid_t)(input.ReadLong());
  delete_location.offset = (oid_t)(input.ReadLong());
}

// Used for write behind logging
size_t TupleRecord::GetTupleRecordSize(void) {
  // log_record_type + header_legnth + db_oid + table_oid + txn_id +
  // insert_location + delete_location
  return sizeof(char) + sizeof(int) + sizeof(oid_t) + sizeof(oid_t) +
      sizeof(txn_id_t) + sizeof(ItemPointer) * 2;
}

// just for debugging
void TupleRecord::Print() {
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
