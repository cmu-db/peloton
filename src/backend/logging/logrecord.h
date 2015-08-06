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
#include "backend/concurrency/transaction.h"

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
            oid_t database_oid,
            const concurrency::Transaction *transaction,
            // tile group id or table id ...
            const char* serialized_data,
            size_t serialized_data_size) 
  : log_record_type(log_record_type),
    database_oid(database_oid),
    transaction(transaction),
    serialized_data(serialized_data),
    serialized_data_size(serialized_data_size)
    {
      assert(log_record_type != LOGRECORD_TYPE_INVALID);
      assert(database_oid != INVALID_OID);
      assert(transaction !=  nullptr);
      assert(serialized_data != nullptr); 
      assert(serialized_data_size > 0); 
    };

  LogRecordType GetType() const;

  oid_t GetDbOid() const;

  const concurrency::Transaction* GetTxn() const;

  const char* GetData() const;

  size_t GetDataSize() const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:
  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  oid_t database_oid = INVALID_OID;

  const concurrency::Transaction *transaction;

  const char* serialized_data;

  size_t serialized_data_size;
};

}  // namespace logging
}  // namespace peloton
