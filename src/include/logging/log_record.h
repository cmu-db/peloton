//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record.h
//
// Identification: src/logging/log_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "common/item_pointer.h"
#include "common/macros.h"
#include "type/value.h"
#include "codegen/value.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord {
  friend class LogRecordFactory;
private:
  LogRecord(LogRecordType log_type, const ItemPointer &pos, 
            const eid_t epoch_id, const txn_id_t txn_id, const cid_t commit_id)
    : log_record_type_(log_type), 
      tuple_pos_(pos), 
      eid_(epoch_id),
      txn_id_(txn_id),
      cid_(commit_id) {}

public:
  virtual ~LogRecord() {}

  inline LogRecordType GetType() const { return log_record_type_; }

  inline void SetItemPointer(const ItemPointer &pos) { tuple_pos_ = pos; }

  inline void SetOldItemPointer(const ItemPointer &pos) { old_tuple_pos_ = pos; }

  inline void SetEpochId(const eid_t epoch_id) { eid_ = epoch_id; }

  inline void SetCommitId(const cid_t commit_id) { cid_ = commit_id; }

  inline void SetTransactionId(const txn_id_t txn_id) { txn_id_ = txn_id; }

  inline void SetDiffVector(char *diff_array, uint32_t diff_size) {
    diff_array_ = diff_array;
    diff_size_ = diff_size;
  }

  inline const ItemPointer &GetItemPointer() { return tuple_pos_; }

  inline const ItemPointer &GetOldItemPointer() { return old_tuple_pos_; }

  inline eid_t GetEpochId() { return eid_; }

  inline cid_t GetCommitId() { return cid_; }

  inline char *GetDiffArray() { return diff_array_; }

  inline uint32_t GetDiffSize() { return diff_size_; }

  inline txn_id_t GetTransactionId() { return txn_id_; }

private:
  LogRecordType log_record_type_;

  ItemPointer tuple_pos_;

  ItemPointer old_tuple_pos_;

  eid_t eid_;

  txn_id_t txn_id_;

  cid_t cid_;

  char *diff_array_;

  uint32_t diff_size_;
};


class LogRecordFactory {
public:

  static LogRecord CreateTupleRecord(const LogRecordType log_type,
                                     const ItemPointer &pos, eid_t current_eid,
                                     txn_id_t txn_id, cid_t current_cid) {
    PL_ASSERT(log_type == LogRecordType::TUPLE_INSERT ||
              log_type == LogRecordType::TUPLE_DELETE ||
              log_type == LogRecordType::TUPLE_UPDATE);
    return LogRecord(log_type, pos, current_eid, txn_id, current_cid);
  }
//  static LogRecord CreateTupleRecord(const LogRecordType log_type, const ItemPointer &pos) {
//    PL_ASSERT(log_type == LogRecordType::TUPLE_INSERT ||
//              log_type == LogRecordType::TUPLE_DELETE ||
//              log_type == LogRecordType::TUPLE_UPDATE);
//    return LogRecord(log_type, pos, INVALID_EID, INVALID_CID);
//  }
//
//  static LogRecord CreateTxnRecord(const LogRecordType log_type, const cid_t commit_id) {
//    PL_ASSERT(log_type == LogRecordType::TRANSACTION_BEGIN ||
//              log_type == LogRecordType::TRANSACTION_COMMIT);
//    return LogRecord(log_type, INVALID_ITEMPOINTER, INVALID_EID, commit_id);
//  }
//
//  static LogRecord CreateEpochRecord(const LogRecordType log_type, const eid_t epoch_id) {
//    PL_ASSERT(log_type == LogRecordType::EPOCH_BEGIN ||
//              log_type == LogRecordType::EPOCH_END);
//    return LogRecord(log_type, INVALID_ITEMPOINTER, epoch_id, INVALID_CID);
//  }
};

}  // namespace logging
}  // namespace peloton
