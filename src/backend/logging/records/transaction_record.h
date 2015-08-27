#pragma once

#include "backend/logging/log_record.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TransactionRecord
//===--------------------------------------------------------------------===//

class TransactionRecord : public LogRecord{

public:
  TransactionRecord( LogRecordType log_record_type,
                     const txn_id_t txn_id = INVALID_TXN_ID,
                     oid_t db_oid = INVALID_OID)
  : LogRecord(log_record_type), txn_id(txn_id)
  {
    if( db_oid == INVALID_OID){
      db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    }
  }

  ~TransactionRecord(){
    if( message_length > 0 ){
      free(message);
    }
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize();

  void Deserialize(CopySerializeInput& input);

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  txn_id_t GetTxnId() const{ return txn_id; }

  void Print(void);

private:

  txn_id_t txn_id;

  oid_t db_oid;

};

}  // namespace logging
}  // namespace peloton
