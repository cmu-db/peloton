//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record.h
//
// Identification: src/backend/logging/log_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/* The following entry types are distinguished:
 *
 * Possible Log Entries:
 *
 *     Transaction Record :
 *       - LogRecordType         : enum
 *     - HEADER
 *       - Header length         : int
 *       - Transaction Id        : txn_id_t
 *
 *     Tuple Record :
 *       - LogRecordType         : enum
 *     -HEADER
 *       - Header length         : int
 *       - Database Oid          : oid_t
 *       - Table Oid             : oid_t
 *       - Transaction Id        : txn_id_t
 *       - Inserted Location     : ItemPointer
 *       - Deleted Location      : ItemPointer
 *     -BODY
 *       - Body length           : int
 *       - Data                  : void*
*/

#pragma once

#include "backend/common/types.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord {
 public:
  LogRecord(LogRecordType log_record_type, cid_t cid)
      : log_record_type(log_record_type), cid(cid) {
    assert(log_record_type != LOGRECORD_TYPE_INVALID);
  }

  virtual ~LogRecord() {}

  LogRecordType GetType() const { return log_record_type; }

  cid_t GetTransactionId() const { return cid; }

  virtual bool Serialize(CopySerializeOutput &output) = 0;

  char *GetMessage(void) const { return message; }

  size_t GetMessageLength(void) const { return message_length; }

 protected:
  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  cid_t cid;

  // serialized message
  char *message = nullptr;

  // length of the message
  size_t message_length = 0;
};

}  // namespace logging
}  // namespace peloton
