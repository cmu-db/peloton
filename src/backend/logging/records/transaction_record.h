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
                     const txn_id_t txn_id = INVALID_TXN_ID,
                     oid_t db_oid = INVALID_OID)
 : LogRecord(log_record_type),
   txn_id(txn_id),
   db_oid(db_oid) {
    // Set the db oid
    if( db_oid == INVALID_OID){
      db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    }
 }

  ~TransactionRecord(){
    // Clean up the message
    free(message);
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize();

  void Deserialize(CopySerializeInput& input);

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  txn_id_t GetTransactionId() const{ return txn_id; }

  void Print(void);

 private:

  // transaction id
  txn_id_t txn_id;

  // TODO: Do we need this ?
  // db oid
  oid_t db_oid;

};

}  // namespace logging
}  // namespace peloton
