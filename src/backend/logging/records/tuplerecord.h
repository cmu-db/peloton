/*-------------------------------------------------------------------------
 *
 * tuplerecord.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/records/tuplerecord.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logrecord.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TupleRecord
//===--------------------------------------------------------------------===//

class TupleRecord : public LogRecord{

public:

  TupleRecord( LogRecordType log_record_type)
  : LogRecord(log_record_type)
  {
    txn_id = INVALID_TXN_ID;
    db_oid = INVALID_OID;
    table_oid = INVALID_OID;
    memset(&insert_location, 0, sizeof(ItemPointer));
    memset(&delete_location, 0, sizeof(ItemPointer));
    data = nullptr;
  }

  TupleRecord( LogRecordType log_record_type,
               const txn_id_t txn_id,
               oid_t table_oid,
               ItemPointer insert_location,
               ItemPointer delete_location,
               const void* data = nullptr,
               oid_t _db_oid = INVALID_OID
               )
  : LogRecord(log_record_type), 
    txn_id(txn_id), 
    table_oid(table_oid), 
    insert_location(insert_location),
    delete_location(delete_location),
    data(data),
    db_oid(_db_oid)
  {
    assert(txn_id);
    if( db_oid == INVALID_OID){
      db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    }
    assert(db_oid);
    assert(table_oid);
  }

  ~TupleRecord(){
    if( serialized_data_size > 0 ){
      free(serialized_data);
    }
  } 

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize();

  void SerializeHeader(CopySerializeOutput& output);

  void DeserializeHeader(CopySerializeInput& input);

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  oid_t GetDbId() const{ return db_oid; }

  txn_id_t GetTxnId() const{ return txn_id; }
 
  oid_t GetTableId(void) const {return table_oid;}

  ItemPointer GetInsertLocation(void) const {return insert_location;}

  ItemPointer GetDeleteLocation(void) const {return delete_location;}

  void print(void);

private:
  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  txn_id_t txn_id;

  oid_t table_oid;

  ItemPointer insert_location;

  ItemPointer delete_location;

  const void* data;

  oid_t db_oid;

};

}  // namespace logging
}  // namespace peloton
