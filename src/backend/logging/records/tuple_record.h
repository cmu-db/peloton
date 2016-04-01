//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_record.h
//
// Identification: src/backend/logging/records/tuple_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/log_record.h"
#include "backend/storage/tuple.h"
#include "backend/common/serializer.h"
#include "backend/common/printable.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TupleRecord
//===--------------------------------------------------------------------===//

class TupleRecord : public LogRecord, Printable {
 public:
  TupleRecord(LogRecordType log_record_type)
      : LogRecord(log_record_type, INVALID_CID) {
    db_oid = INVALID_OID;
    table_oid = INVALID_OID;

    data = nullptr;
  }

  TupleRecord(LogRecordType log_record_type, const cid_t cid,
              oid_t table_oid, ItemPointer insert_location,
              ItemPointer delete_location, const void *data = nullptr,
              oid_t _db_oid = INVALID_OID)
      : LogRecord(log_record_type, cid),
        table_oid(table_oid),
        insert_location(insert_location),
        delete_location(delete_location),
        data(data),
        db_oid(_db_oid) {
    assert(cid);
    assert(table_oid);

    if (db_oid == INVALID_OID) {
      db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    }
    assert(db_oid);
  }

  ~TupleRecord() {
    // Clean up the message
    delete[] message;
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization
  //===--------------------------------------------------------------------===//

  bool Serialize(CopySerializeOutput &output);

  void SerializeHeader(CopySerializeOutput &output);

  void DeserializeHeader(CopySerializeInputBE &input);

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  oid_t GetDatabaseOid() const { return db_oid; }

  oid_t GetTableId(void) const { return table_oid; }

  ItemPointer GetInsertLocation(void) const { return insert_location; }

  ItemPointer GetDeleteLocation(void) const { return delete_location; }

  void SetTuple(storage::Tuple *tuple);

  storage::Tuple *GetTuple();

  static size_t GetTupleRecordSize(void);

  // Get a string representation for debugging
  const std::string GetInfo() const;

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
  const void *data;

  // tuple (for deserialize
  storage::Tuple *tuple = nullptr;

  // database id
  oid_t db_oid;
};

}  // namespace logging
}  // namespace peloton
