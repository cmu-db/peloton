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
  {}

  TupleRecord( LogRecordType log_record_type,
               const txn_id_t txn_id,
               oid_t table_oid,
               ItemPointer itemPointer,
               const void* data)
  : LogRecord(log_record_type), 
    txn_id(txn_id), 
    table_oid(table_oid), 
    itemPointer(itemPointer),
    data(data)
  {
    assert(txn_id);
    db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    assert(db_oid);
    assert(table_oid);
    Serialize();
  }

  ~TupleRecord(){
    if( serialized_data_size > 0 )
      free(serialized_data);
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

  ItemPointer GetItemPointer(void) const {return itemPointer;}

  friend std::ostream& operator<<(std::ostream& os, const TupleRecord& tupleRecord);

private:
  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  oid_t db_oid;

  txn_id_t txn_id;

  oid_t table_oid;

  ItemPointer itemPointer;

  const void* data;

};

}  // namespace logging
}  // namespace peloton
