//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record.h
//
// Identification: src/logging/log_record.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include "common/internal_types.h"
#include "common/item_pointer.h"
#include "common/macros.h"
#include "type/value.h"
#include "../common/internal_types.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord {
  friend class LogRecordFactory;
private:
  LogRecord(LogRecordType log_type, const ItemPointer &old_pos, const ItemPointer &pos,
            const eid_t epoch_id, const txn_id_t txn_id, const cid_t commit_id, oid_t schema_id)
    : log_record_type_(log_type),
      old_tuple_pos_(old_pos),
      tuple_pos_(pos), 
      eid_(epoch_id),
      txn_id_(txn_id),
      cid_(commit_id),
      schema_id_(schema_id) {}

public:
  virtual ~LogRecord() {}

  inline LogRecordType GetType() const { return log_record_type_; }

  inline void SetItemPointer(const ItemPointer &pos) { tuple_pos_ = pos; }

  inline void SetOldItemPointer(const ItemPointer &pos) { old_tuple_pos_ = pos; }

  inline void SetEpochId(const eid_t epoch_id) { eid_ = epoch_id; }

  inline void SetCommitId(const cid_t commit_id) { cid_ = commit_id; }

  inline void SetTransactionId(const txn_id_t txn_id) { txn_id_ = txn_id; }

  inline void SetValuesArray(char *diff_array, uint32_t num_values) {
    values_ = diff_array;
    num_values_ = num_values;
  }

  inline void SetOffsetsArray(TargetList *arr) {
    offsets_ = arr;
  }

  inline void SetSchemaId(oid_t schema_id) {
    schema_id_ = schema_id;
  }

  inline const ItemPointer &GetItemPointer() { return tuple_pos_; }

  inline const ItemPointer &GetOldItemPointer() { return old_tuple_pos_; }

  inline eid_t GetEpochId() { return eid_; }

  inline cid_t GetCommitId() { return cid_; }

  inline char *GetValuesArray() { return values_; }

  inline uint32_t GetNumValues() { return num_values_; }

  inline TargetList *GetOffsets() { return offsets_; }

  inline txn_id_t GetTransactionId() { return txn_id_; }

  inline oid_t GetSchemaId() { return schema_id_; }

private:
  LogRecordType log_record_type_;

  ItemPointer old_tuple_pos_;

  ItemPointer tuple_pos_;

  eid_t eid_;

  txn_id_t txn_id_;

  cid_t cid_;

  char *values_;

  TargetList *offsets_;

  uint32_t num_values_;

  oid_t schema_id_;
};


class LogRecordFactory {
public:

  static LogRecord CreateTupleRecord(const LogRecordType log_type,
                                     const ItemPointer &pos, eid_t current_eid,
                                     txn_id_t txn_id, cid_t current_cid, oid_t schema_oid) {
    PELOTON_ASSERT(log_type == LogRecordType::TUPLE_INSERT ||
              log_type == LogRecordType::TUPLE_DELETE);

    return LogRecord(log_type, INVALID_ITEMPOINTER, pos, current_eid, txn_id, current_cid, schema_oid);
  }

  static LogRecord CreateTupleRecord(const LogRecordType log_type, eid_t current_eid,
                                     txn_id_t txn_id, cid_t current_cid)  {

    PELOTON_ASSERT(log_type == LogRecordType::TRANSACTION_COMMIT ||
              log_type == LogRecordType::TRANSACTION_ABORT  ||
              log_type == LogRecordType::TRANSACTION_BEGIN);

    return LogRecord(log_type, INVALID_ITEMPOINTER, INVALID_ITEMPOINTER,
                     current_eid, txn_id, current_cid, INVALID_OID);
  }

  static LogRecord CreateTupleRecord(const LogRecordType log_type, const ItemPointer &old_pos,
                                     const ItemPointer &pos, eid_t current_eid,
                                     txn_id_t txn_id, cid_t current_cid, oid_t schema_oid) {

    PELOTON_ASSERT(log_type == LogRecordType::TUPLE_UPDATE);

    return LogRecord(log_type, old_pos, pos, current_eid, txn_id, current_cid, schema_oid);
  }

};

}  // namespace logging
}  // namespace peloton
