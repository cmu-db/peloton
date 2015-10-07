#include "log_record_pool.h"

namespace peloton {
namespace logging {

void LogRecordList::Clear() {
  // Clean up
  LogRecordNode *cur = head_node;
  while (cur != NULL) {
    LogRecordNode *deletingNode = cur;
    cur = cur->next_node;
    head_node = cur;
    _backend->Free(deletingNode);
  }
  tail_node = NULL;
}

void LogRecordList::AddLogRecord(TupleRecord *record) {
  if (_backend == NULL)
    return;

  LogRecordNode *localRecord = (LogRecordNode*) _backend->Allocate(sizeof(LogRecordNode));
  if (record != NULL || record->GetTransactionId() == txn_id) {
    localRecord->_db_oid = record->GetDatabaseOid();
    localRecord->_delete_location = record->GetDeleteLocation();
    localRecord->_insert_location = record->GetInsertLocation();
    localRecord->_log_record_type = record->GetType();
    localRecord->_table_oid = record->GetTableId();
    localRecord->next_node = NULL;

    // add to the list
    if (tail_node == NULL) {
      head_node = localRecord;
    } else {
      tail_node->next_node = localRecord;
    }
    tail_node = localRecord;
  }
}

void LogRecordPool::Clear() {
  // Clean up
  LogRecordList *cur = head_list;
  while (cur != NULL) {
    LogRecordList *deletingNode = cur;
    cur = cur->next_list;
    head_list = cur;
    RemoveLogList(NULL, deletingNode);
  }
  tail_list = NULL;
}

void LogRecordPool::CreateTxnLogList(txn_id_t txn_id) {
  if (_backend == NULL) {
    return;
  }
  LogRecordList* existing_list = SearchRecordList(txn_id);
  if (existing_list == NULL) {
    existing_list = (LogRecordList*) _backend->Allocate(sizeof(LogRecordList));
    existing_list->init(_backend);
    existing_list->txn_id = txn_id;

    // add to the pool
    if (tail_list == NULL) {
      head_list = existing_list;
    } else {
      tail_list->next_list = existing_list;
    }
    tail_list = existing_list;
  }
}

void LogRecordPool::AddLogRecord(TupleRecord *record) {
  LogRecordList* existing_list = SearchRecordList(record->GetTransactionId());
  if (existing_list == NULL) {
    existing_list->AddLogRecord(record);
  }
}

void LogRecordPool::RemoveTxnLogList(txn_id_t txn_id) {
  LogRecordList *prev = NULL;
  LogRecordList *cur = head_list;
  while (cur != NULL) {
    if (cur->GetTxnID() == txn_id) {
      RemoveLogList(prev, cur);
      break;
    }
    prev = cur;
    cur = cur->next_list;
  }
}

LogRecordList* LogRecordPool::SearchRecordList(txn_id_t txn_id) {
  LogRecordList *cur = head_list;
  while (cur != NULL) {
    if (cur->GetTxnID() == txn_id) {
      return cur;
    }
    cur = cur->next_list;
  }
  return NULL;
}

void LogRecordPool::RemoveLogList(LogRecordList *prev, LogRecordList *list) {
  if (prev == NULL) {
    head_list = list->next_list;
  } else {
    prev->next_list = list->next_list;
  }
  if (list->next_list == NULL) {
    tail_list = prev;
  }
  list->Clear();
  _backend->Free(list);
}
}
}
