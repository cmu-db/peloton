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
            const concurrency::Transaction *transaction,
            oid_t table_oid,
            const void* data) 
  : log_record_type(log_record_type), transaction(transaction), table_oid(table_oid)
  {
    assert(log_record_type != LOGRECORD_TYPE_INVALID);
    database_oid = bridge::Bridge::GetCurrentDatabaseOid(),
    assert(database_oid != INVALID_OID);
    assert(table_oid != INVALID_OID);
    //TODO :: Maybe, we dont' need all information ...
    assert(transaction !=  nullptr);
    assert(SerializeData(data));
  }

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  LogRecordType GetType() const;

  oid_t GetDbOid() const;

  oid_t GetTableOid() const;

  const concurrency::Transaction* GetTxn() const;

  const char* GetData() const;

  size_t GetDataSize() const;

  friend std::ostream &operator<<(std::ostream &os, const LogRecord& record);

private:
  bool SerializeData(const void* data);

  LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

  const concurrency::Transaction *transaction;

  oid_t database_oid = INVALID_OID;

  oid_t table_oid = INVALID_OID;

  char* serialized_data;

  size_t serialized_data_size;

};

}  // namespace logging
}  // namespace peloton
