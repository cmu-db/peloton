/*-------------------------------------------------------------------------
 *
 * logrecord.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logrecord.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/bridge/ddl/bridge.h"

#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord{

public:

  LogRecord() = delete;

  LogRecord(LogRecordType log_record_type,
            const txn_id_t txn_id,
            oid_t table_oid,
            ItemPointer itemPointer,
            const void* data) 
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
    assert(SerializeLogRecord(data));
  }

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  static size_t GetSerializedLogRecordHeaderSize(void);

  char* GetSerializedLogRecord(void) const;

  size_t GetSerializedLogRecordSize(void) const;

  LogRecordType GetType(void) const;

  oid_t GetDbId(void) const;
  
  oid_t GetTableId(void) const;

  txn_id_t GetTxnId(void) const;

  ItemPointer GetItemPointer(void) const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:

  void SerializeLogRecordHeader(CopySerializeOutput& output);

  bool SerializeLogRecord(const void* data);

  void DeserializeLogRecordHeader(CopySerializeInput& input);

  bool DeserializeLogRecord(const void* data, size_t data_size);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  txn_id_t txn_id;

  oid_t table_oid;

  ItemPointer itemPointer;

  oid_t db_oid;

  char* serialized_log_record;

  size_t serialized_log_record_size;

};

}  // namespace logging
}  // namespace peloton
