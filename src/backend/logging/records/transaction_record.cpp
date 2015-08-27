
#include <iostream>

#include "backend/logging/records/transaction_record.h"

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
  
  message_length = output.Size();
  message = (char*)malloc(message_length);
  memset( message, 0, message_length);
  memcpy( message, output.Data(), message_length);

  return status;
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input  
 */
void TransactionRecord::Deserialize(CopySerializeInput& input){
  input.ReadInt();
  txn_id = (txn_id_t)(input.ReadLong());
}

void TransactionRecord::Print(void){
  std::cout << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
  std::cout << " #Txn ID:" << GetTxnId() << "\n";
  std::cout << "\n";
}

}  // namespace logging
}  // namespace peloton
