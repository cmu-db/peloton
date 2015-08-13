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

/*
 * LogRecord Format:
 *
 * Every entry has the structure LogRecordHeader [+ serialized_data]
 *
 * The following entry types are distinguished:
 *
 * Possible Log Entries:
 *     Transaction Entries:
 *       - LogRecordHeader      : LogRecordType, database_oid, txn_id, table_id, (value)
 *       - length(value)        : sizeof(int)
 *       - value_id             : sizeof(value_id_t)
 *       - field_id             : sizeof(field_id_t)
 *       - table_name           : table_name.size()
 *       - table_name.size()    : sizeof(char)
 *       - type ("D")           : sizeof(char)
 *
 *     Tuple Entries:
 *       - list of value_ids    : store->columnCount()
 *       - row_id               : sizeof(pos_t)
 *       - table_name           : table_name.size()
 *       - table_name.size()    : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - type ("V")           : sizeof(char)
 *
 *     Invalidation Entries:
 *       - invalidated_row_id   : sizeof(pos_t)
 *       - table_name           : table_name.size()
 *       - table_name.size()    : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - type ("I")           : sizeof(char)
 *
 *     Commit Entries:
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - type ("C")           : sizeof(char)
 *
 *     Rollback Entries:
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - type ("R")           : sizeof(char)
 *
 *     Skip Entries:
 *       - padding              : bytes filled with 255. Used to align log to BLOCKSIZE (alignment from beginning of
 *file)
 *       - type (255)           : sizeof(char)
 *
 *     Checkpoint Start
 *       - checkpoint id        : sizeof(int)
 *       - type ("X")           : sizeof(char)
 *
 *     Checkpoint End
 *       - checkpoint id        : sizeof(int)
 *       - type ("Y")           : sizeof(char)
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/logging/logrecordheader.h"

#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord{

public:

  // INSERT/UPDATE TUPLE
  LogRecord(LogRecordHeader log_record_header, const void* data) 
  : log_record_header(log_record_header), data(data) { }

  // DELETE_TUPLE
  LogRecord(LogRecordHeader log_record_header) 
  : log_record_header(log_record_header){data = nullptr;}

  //===--------------------------------------------------------------------===//
  // Serialization 
  //===--------------------------------------------------------------------===//

  bool SerializeLogRecord();

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  LogRecordHeader GetHeader() const;

  char* GetSerializedLogRecord(void) const;

  size_t GetSerializedLogRecordSize(void) const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  LogRecordHeader log_record_header;

  const void* data;

  char* serialized_log_record;

  size_t serialized_log_record_size;

};

}  // namespace logging
}  // namespace peloton
