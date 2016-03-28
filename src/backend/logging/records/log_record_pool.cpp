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
  for (auto it = txn_log_table.begin(); it != txn_log_table.end(); it++) {
    std::vector<TupleRecord *> list = it->second;

    for (auto record : list) {
      delete record;
    }
  }

  txn_log_table.clear();
}

int LogRecordPool::CreateTransactionLogList(txn_id_t txn_id) {
  // Create a new transaction log list if not already found
  auto existing_list = SearchLogRecordList(txn_id);

  if (existing_list == nullptr) {
    txn_log_table.insert(std::pair<txn_id_t, std::vector<TupleRecord *>>(
        txn_id, std::vector<TupleRecord *>()));
  }

  return 0;
}

int LogRecordPool::AddLogRecord(TupleRecord *record) {
  // Locate the transaction log list
  auto existing_list = SearchLogRecordList(record->GetTransactionId());

  // Add to the transaction log list if found
  if (existing_list != nullptr) {
    existing_list->push_back(record);
    return 0;
  }

  return -1;
}

void LogRecordPool::RemoveTransactionLogList(txn_id_t txn_id) {
  // Locate the transaction log list
  if (txn_log_table.find(txn_id) != txn_log_table.end()) {
    // Clean the log records in the transaction log list
    std::vector<TupleRecord *> list = txn_log_table.at(txn_id);

    for (auto record : list) {
      delete record;
    }

    // Remove the transaction log list
    txn_log_table.erase(txn_id);
  }
}

std::vector<TupleRecord *> *LogRecordPool::SearchLogRecordList(
    txn_id_t txn_id) {
  // Locate the transaction log list
  if (txn_log_table.find(txn_id) != txn_log_table.end()) {
    return &(txn_log_table.at(txn_id));
  } else {
    return nullptr;
  }
}
}
}
