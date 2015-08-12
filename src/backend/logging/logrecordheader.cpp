#include "backend/common/logger.h"
#include "backend/logging/logrecordheader.h"

#include <iostream>

namespace peloton {
namespace logging {

/**
 * @brief Serialize LogRecordHeader
 * @param output  
 */
void LogRecordHeader::SerializeLogRecordHeader(CopySerializeOutput& output){
  size_t start = output.Position();
  output.WriteInt(0);  // reserve first 4 bytes for the header size
  output.WriteEnumInSingleByte(log_record_type);
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
void LogRecordHeader::DeserializeLogRecordHeader(CopySerializeInput& input){
  log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  db_oid = (oid_t)(input.ReadShort());
  table_oid = (oid_t)(input.ReadShort());
  txn_id = (txn_id_t)(input.ReadLong());
  itemPointer.block = (oid_t)(input.ReadShort());
  itemPointer.offset = (oid_t)(input.ReadShort());
}

LogRecordType LogRecordHeader::GetType() const{
  return log_record_type;
}

oid_t LogRecordHeader::GetDbId() const{
  return db_oid;
}

oid_t LogRecordHeader::GetTableId() const{
  return table_oid;
}

txn_id_t LogRecordHeader::GetTxnId() const{
  return txn_id;
}

ItemPointer LogRecordHeader::GetItemPointer() const{
  return itemPointer;
}

/**
 * @brief Read first 4 bytes to get header size
 * @param fp FilePointer to read 4 bytes  
 */
size_t LogRecordHeader::GetSerializedHeaderSize(FILE* fp){
  char buffer[4];
  int ret = fread(buffer, 1, sizeof(buffer), fp);
  if( ret <= 0 ){
    return 0;
  }

  CopySerializeInput input(buffer, sizeof(buffer));
  header_size = (size_t)input.ReadInt();
  return header_size;
}

std::ostream& operator<<(std::ostream& os, const LogRecordHeader& header) {
  os << "#LOG TYPE:" << LogRecordTypeToString(header.GetType()) << "\n";
  os << " #Db  ID:" << header.GetDbId() << "\n";
  os << " #Tb  ID:" << header.GetTableId() << "\n";
  os << " #Txn ID:" << header.GetTxnId() << "\n";
  os << " #Location :" << header.GetItemPointer().block;
  os << " " << header.GetItemPointer().offset << "\n";
  os << "\n";
  return os;
}

}  // namespace logging
}  // namespace peloton


