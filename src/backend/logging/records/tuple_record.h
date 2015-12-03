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

#include "backend/logging/log_record.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TupleRecord
//===--------------------------------------------------------------------===//

class TupleRecord : public LogRecord{

public:

  TupleRecord( LogRecordType log_record_type)
  : LogRecord(log_record_type, INVALID_TXN_ID) {
    db_oid = INVALID_OID;
    table_oid = INVALID_OID;

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
  : LogRecord(log_record_type, txn_id), 
    table_oid(table_oid), 
    insert_location(insert_location),
    delete_location(delete_location),
    data(data),
    db_oid(_db_oid){

    assert(txn_id);
    assert(table_oid);

    if( db_oid == INVALID_OID){
      db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    }
    assert(db_oid);

  }

  ~TupleRecord(){
    // Clean up the message
    delete message;
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization 
  //===--------------------------------------------------------------------===//

  bool Serialize(CopySerializeOutput& output);

  void SerializeHeader(CopySerializeOutput& output);

  void DeserializeHeader(CopySerializeInputBE& input);

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  oid_t GetDatabaseOid() const{ return db_oid; }

  oid_t GetTableId(void) const {return table_oid;}

  ItemPointer GetInsertLocation(void) const {return insert_location;}

  ItemPointer GetDeleteLocation(void) const {return delete_location;}

  static size_t GetTupleRecordSize(void);

  void Print(void);

private:

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // table id
  oid_t table_oid;

  // inserted tuple location
  ItemPointer insert_location;

  // deleted tuple location
  ItemPointer delete_location;

  // message
  const void* data;

  // database id
  oid_t db_oid;

};

}  // namespace logging
}  // namespace peloton
