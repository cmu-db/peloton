//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record_pool.cpp
//
// Identification: src/backend/logging/records/log_record_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "log_record_pool.h"

namespace peloton {
namespace logging {

void LogRecordPool::Clear() {
  // Clean up
  txn_log_table.clear();
}

int LogRecordPool::CreateTxnLogList(txn_id_t txn_id) {
  // Create a new transaction log list if not already found
  auto existing_list = ExistsTxnLogRecordList(txn_id);

  if (existing_list == false) {
    txn_log_table.insert(
        std::pair<txn_id_t, std::vector<std::unique_ptr<TupleRecord>>>(
        txn_id, std::vector<std::unique_ptr<TupleRecord>>()));
  }

  return 0;
}

int LogRecordPool::AddLogRecord(std::unique_ptr<TupleRecord> record) {
  // Get the record transaction id
  auto record_transaction_id = record->GetTransactionId();

  // Locate the transaction log list
  auto existing_list = ExistsTxnLogRecordList(record_transaction_id);

  // Add to the transaction log list if found
  if (existing_list != false) {
    txn_log_table.at(record_transaction_id).push_back(std::move(record));
    return 0;
  }

  return -1;
}

void LogRecordPool::RemoveTxnLogRecordList(txn_id_t txn_id) {
  // Locate the transaction log list
  if (txn_log_table.find(txn_id) != txn_log_table.end()) {

    // Erase transaction log record list and its contents
    txn_log_table.erase(txn_id);
  }
}

bool LogRecordPool::ExistsTxnLogRecordList(txn_id_t txn_id) {
  // Locate the transaction log list
  if (txn_log_table.find(txn_id) != txn_log_table.end()) {
    return true;
  } else {
    return false;
  }
}

}
}
