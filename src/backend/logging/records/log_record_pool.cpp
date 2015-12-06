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

int LogRecordPool::CreateTxnLogList(txn_id_t txn_id) {
  auto existing_list = SearchRecordList(txn_id);
  if (existing_list == nullptr) {
    txn_log_table.insert(
        std::pair<txn_id_t, std::vector<TupleRecord *>>(
            txn_id, std::vector<TupleRecord *>()));
  }
  return 0;
}

int LogRecordPool::AddLogRecord(TupleRecord *record) {
  auto existing_list = SearchRecordList(record->GetTransactionId());
  if (existing_list != nullptr) {
    existing_list->push_back(record);
    return 0;
  }
  return -1;
}

void LogRecordPool::RemoveTxnLogList(txn_id_t txn_id) {
  if (txn_log_table.find(txn_id) != txn_log_table.end()) {
    // clean data in list
    std::vector<TupleRecord *> list = txn_log_table.at(txn_id);
    for (auto record : list) {
      delete record;
    }
    // remove the list
    txn_log_table.erase(txn_id);
  }
}

std::vector<TupleRecord *>* LogRecordPool::SearchRecordList(txn_id_t txn_id) {
  if (txn_log_table.find(txn_id) != txn_log_table.end()) {
    return &(txn_log_table.at(txn_id));
  } else {
    return nullptr;
  }
}

}
}
