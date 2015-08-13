/*-------------------------------------------------------------------------
 *
 * logrecordheader.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logrecordheader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/bridge/ddl/bridge.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord Header
//===--------------------------------------------------------------------===//

class LogRecordHeader{

public:

  LogRecordHeader(){}

  // TODO :: make other constructors for txn logging

  LogRecordHeader(LogRecordType log_record_type,
                  const txn_id_t txn_id,
                  oid_t table_oid,
                  ItemPointer itemPointer)
  : log_record_type(log_record_type),
    txn_id(txn_id), 
    table_oid(table_oid), 
    itemPointer(itemPointer)
  {
    assert(log_record_type != LOGRECORD_TYPE_INVALID);
    db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    assert(db_oid);
    assert(table_oid);
    assert(txn_id != INVALID_TXN_ID );
  }

  void SerializeLogRecordHeader(CopySerializeOutput& output);

  void DeserializeLogRecordHeader(CopySerializeInput& input);

  LogRecordType GetType(void) const;

  oid_t GetDbId(void) const;
  
  oid_t GetTableId(void) const;

  txn_id_t GetTxnId(void) const;

  ItemPointer GetItemPointer(void) const;

  size_t GetSerializedHeaderSize(FILE* fp);

  friend std::ostream &operator<<(std::ostream &os, const LogRecordHeader& header);

private:

  size_t header_size;

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  txn_id_t txn_id;

  oid_t table_oid;

  ItemPointer itemPointer;

  oid_t db_oid;

};

}  // namespace logging
}  // namespace peloton


