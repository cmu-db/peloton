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

class LogRecord{

 public:

  LogRecord(LogRecordType log_record_type, txn_id_t txn_id) 
 : log_record_type(log_record_type), txn_id(txn_id) {
    assert(log_record_type != LOGRECORD_TYPE_INVALID);
  }

  virtual ~LogRecord(){}

  LogRecordType GetType() const{ return log_record_type; }

  txn_id_t GetTransactionId() const{ return txn_id; }

  virtual bool Serialize(CopySerializeOutput& output) = 0;

  virtual void Print(void) = 0;

  char* GetMessage(void) const {return message;}

  size_t GetMessageLength(void) const {return message_length;}

 protected:

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  txn_id_t txn_id;

  // serialized message
  char* message = nullptr;

  // length of the message
  size_t message_length = 0;

};

}  // namespace logging
}  // namespace peloton
