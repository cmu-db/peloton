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

#include "tuple_record.h"
#include "backend/storage/abstract_backend.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TupleRecord Log linked list node
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

  LogRecordNode *next_node;
} LogRecordNode;

//===--------------------------------------------------------------------===//
// Log record list
//===--------------------------------------------------------------------===//

class LogRecordList {

 public:
  ~LogRecordList() {
    Clear();
  }

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  void init(storage::AbstractBackend *backend) {
    head_node = NULL;
    next_list = NULL;
    _backend = backend;
    txn_cid = INVALID_CID;
    tail_node = NULL;
  }

  void Clear();

  bool IsEmpty() {
    return head_node == NULL;
  }

  bool IsCommit() {
    return txn_cid != INVALID_CID;
  }

  cid_t GetCommit() {
    return txn_cid;
  }

  void SetCommit(cid_t commit_id) {
    txn_cid = commit_id;
  }

  txn_id_t GetTxnID() {
    return txn_id;
  }

  void AddLogRecord(TupleRecord *record);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//
  LogRecordNode *head_node = NULL;
  LogRecordList *next_list = NULL;
  storage::AbstractBackend *_backend = NULL;
  txn_id_t txn_id;
 private:
  cid_t txn_cid = INVALID_CID;
  LogRecordNode *tail_node = NULL;
};

//===--------------------------------------------------------------------===//
// Log record pool
//===--------------------------------------------------------------------===//

class LogRecordPool {

 public:
  ~LogRecordPool() {
    Clear();
  }

  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//

  void init(storage::AbstractBackend *backend) {
    head_list = NULL;
    _backend = backend;
    tail_list = NULL;
  }

  void Clear();

  bool IsEmpty() {
    return head_list == NULL;
  }

  void CreateTxnLogList(txn_id_t txn_id);

  void AddLogRecord(TupleRecord *record);

  void RemoveTxnLogList(txn_id_t txn_id);

  LogRecordList* SearchRecordList(txn_id_t txn_id);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  LogRecordList *head_list = NULL;
  storage::AbstractBackend *_backend = NULL;

 private:

  void RemoveLogList(LogRecordList *prev, LogRecordList *node);

  LogRecordList *tail_list = NULL;
};

}  // namespace logging
}  // namespace peloton
