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

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize();

  void Deserialize(CopySerializeInput& input);

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  txn_id_t GetTxnId() const{ return txn_id; }

  friend std::ostream &operator<<(std::ostream &os, const TransactionRecord& record);

private:

  txn_id_t txn_id;

};

}  // namespace logging
}  // namespace peloton
