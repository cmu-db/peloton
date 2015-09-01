#include "backend/logging/records/transactionrecord.h"

#include <iostream>

namespace peloton {
namespace logging {

/**
 * @brief Serialize given data
 * @return true if we serialize data otherwise false
 */
bool TransactionRecord::Serialize(){
  bool status = true;

  CopySerializeOutput output;

  output.WriteEnumInSingleByte(log_record_type);
  size_t start = output.Position();
  // then reserve 4 bytes for the header size
  output.WriteInt(0);
  output.WriteLong(txn_id);
  output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
  
  serialized_data_size = output.Size();
  serialized_data = (char*)malloc(serialized_data_size);
  memset( serialized_data, 0, serialized_data_size);
  memcpy( serialized_data, output.Data(), serialized_data_size);

  return status;
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input  
 */
void TransactionRecord::Deserialize(CopySerializeInputBE& input){
  input.ReadInt();
  txn_id = (txn_id_t)(input.ReadLong());
}

void TransactionRecord::print(void){
  std::cout << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
  std::cout << " #Txn ID:" << GetTxnId() << "\n";
  std::cout << "\n";
}

}  // namespace logging
}  // namespace peloton
