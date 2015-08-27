
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

  // First, write out the log record type
  output.WriteEnumInSingleByte(log_record_type);

  // Then reserve 4 bytes for the header size to be written later
  size_t start = output.Position();
  output.WriteInt(0);
  output.WriteLong(txn_id);

  // Write out the header now
  int32_t msg_length = static_cast<int32_t>(output.Position() - start - sizeof(int32_t));
  output.WriteIntAt(start, msg_length);
  
  // TODO: Is this different from msg_length ?
  message_length = output.Size();
  message = (char*) malloc(message_length);
  memset(message, 0, message_length);
  memcpy(message, output.Data(), message_length);

  return status;
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input  
 */
void TransactionRecord::Deserialize(CopySerializeInput& input){

  // Get the message length
  input.ReadInt();

  // Just grab the transaction id
  txn_id = (txn_id_t)(input.ReadLong());

}

void TransactionRecord::Print(void){
  std::cout << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
  std::cout << " #Txn ID:" << GetTransactionId() << "\n";
  std::cout << "\n";
}

}  // namespace logging
}  // namespace peloton
