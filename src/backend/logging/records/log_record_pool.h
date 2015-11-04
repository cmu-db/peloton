/*-------------------------------------------------------------------------
 *
 * log_record_pool.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/records/log_record_pool.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <map>

#include "backend/logging/records/tuple_record.h"
#include "backend/storage/backend.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Linked list node for Tuple Record Log info
//===--------------------------------------------------------------------===//

typedef struct LogRecordNode {
  LogRecordType _log_record_type = LOGRECORD_TYPE_INVALID;
  // table id
  oid_t _table_oid;
  // inserted tuple location
  ItemPointer _insert_location;
  // deleted tuple location
  ItemPointer _delete_location;
  // database id
  oid_t _db_oid;
  // next node in the list
  LogRecordNode *next_node; // need to keep sync
} LogRecordNode;

//===--------------------------------------------------------------------===//
// Log record list for a certain transaction
//===--------------------------------------------------------------------===//

class LogRecordList {

 public:
  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  void init(txn_id_t id) {
    head_node = nullptr;
    next_list = nullptr;
    prev_list = nullptr;
    iscommitting = false;
    tail_node = nullptr;
    txn_id = id;
  }

  void Clear();

  bool IsEmpty() const {
    return head_node == nullptr;
  }

  bool IsCommitting() const {
    return iscommitting;
  }

  void SetCommitting(bool _isCommitting) {
    iscommitting = _isCommitting;
    backend.Sync(this);
  }

  txn_id_t GetTxnID() const {
    return txn_id;
  }

  LogRecordNode* GetHeadNode() const {
    return head_node;
  }

  LogRecordList* GetNextList() const {
    return next_list;
  }

  void SetNextList(LogRecordList* next) {
    next_list = next;
    backend.Sync(this);
  }

  LogRecordList* GetPrevList() const {
    return prev_list;
  }

  void SetPrevList(LogRecordList* prev) {
    prev_list = prev;
    backend.Sync(this);
  }

  int AddLogRecord(TupleRecord *record);

  // recover tail_node and backend if any inconsistency
  void CheckLogRecordList();

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//
 private:
  LogRecordNode *head_node = nullptr; // need to keep sync
  LogRecordList *prev_list = nullptr; // can be corrected after reboot
  LogRecordList *next_list = nullptr; // need to keep sync
  txn_id_t txn_id = INVALID_TXN_ID;

  static storage::Backend& backend;

  bool iscommitting = false; // need to keep sync
  LogRecordNode *tail_node = nullptr; // can be corrected after reboot
};

//===--------------------------------------------------------------------===//
// Log record pool
//===--------------------------------------------------------------------===//

class LogRecordPool {

 public:
  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  void init() {
    head_list = nullptr;
    tail_list = nullptr;
    txn_log_table = new std::map<txn_id_t, LogRecordList *>();
  }

  void Clear(bool doCleanAllLog = false);

  bool IsEmpty() const {
    return head_list == NULL;
  }

  LogRecordList* GetHeadList() const {
    return head_list;
  }

  int CreateTxnLogList(txn_id_t txn_id);

  int AddLogRecord(TupleRecord *record);

  void RemoveTxnLogList(txn_id_t txn_id);

  LogRecordList* SearchRecordList(txn_id_t txn_id) const;

  // recover tail_node and backend if any inconsistency
  void CheckLogRecordPool();

 private:

  void RemoveLogList(LogRecordList *node);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//
  LogRecordList *head_list = nullptr; // need to keep sync
  LogRecordList *tail_list = nullptr; // can be corrected after reboot

  static storage::Backend& backend;

  // Transient record for fast access to log records
  std::map<txn_id_t, LogRecordList *> *txn_log_table = nullptr;
};

}  // namespace logging
}  // namespace peloton
