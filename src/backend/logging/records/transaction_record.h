#pragma once

#include "backend/logging/log_record.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TransactionRecord
//===--------------------------------------------------------------------===//

class TransactionRecord : public LogRecord {

 public:
  TransactionRecord( LogRecordType log_record_type,
                     const txn_id_t txn_id = INVALID_TXN_ID)
 : LogRecord(log_record_type, txn_id) {}

  ~TransactionRecord(){
    // Clean up the message
    free(message);
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize();

  void Deserialize(CopySerializeInputBE& input);

  static size_t GetTransactionRecordSize(void) ;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  void Print(void);

};

}  // namespace logging
}  // namespace peloton
