#pragma once

#include "backend/logging/logrecord.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TransactionRecord
//===--------------------------------------------------------------------===//

class TransactionRecord : public LogRecord{

public:
  TransactionRecord( LogRecordType log_record_type,
                     const txn_id_t txn_id = INVALID_TXN_ID)
  : LogRecord(log_record_type), txn_id(txn_id)
  {}

  ~TransactionRecord(){
    if( serialized_data_size > 0 ){
      free(serialized_data);
    }
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize();

  void Deserialize(CopySerializeInputBE& input);

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  txn_id_t GetTxnId() const{ return txn_id; }

  void print(void);

private:

  txn_id_t txn_id;

};

}  // namespace logging
}  // namespace peloton
