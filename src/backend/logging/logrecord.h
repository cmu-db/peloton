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
            ItemPointer itemPointer)
  : log_record_type(log_record_type), txn_id(txn_id), itemPointer(itemPointer)
  {
    assert(log_record_type != LOGRECORD_TYPE_INVALID);
    assert(txn_id != INVALID_TXN_ID );
  }

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  LogRecordType GetType() const;

  txn_id_t GetTxnId() const;

  ItemPointer GetItemPointer() const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  txn_id_t txn_id;

  ItemPointer itemPointer;

};

}  // namespace logging
}  // namespace peloton
