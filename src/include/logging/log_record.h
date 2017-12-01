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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord {
  friend class LogRecordFactory;

 private:
  LogRecord(LogRecordType log_type, const ItemPointer &pos,
            const eid_t epoch_id, const cid_t commit_id)
      : log_record_type_(log_type),
        tuple_pos_(pos),
        eid_(epoch_id),
        cid_(commit_id) {}

 public:
  virtual ~LogRecord() {}

  inline LogRecordType GetType() const { return log_record_type_; }

  inline void SetItemPointer(const ItemPointer &pos) { tuple_pos_ = pos; }

  inline void SetOldItemPointer(const ItemPointer &pos) {
    old_tuple_pos_ = pos;
  }

  inline void SetEpochId(const eid_t epoch_id) { eid_ = epoch_id; }

  inline void SetCommitId(const cid_t commit_id) { cid_ = commit_id; }

  inline const ItemPointer &GetItemPointer() { return tuple_pos_; }

  inline const ItemPointer &GetOldItemPointer() { return old_tuple_pos_; }

  inline eid_t GetEpochId() { return eid_; }

  inline cid_t GetCommitId() { return cid_; }

 private:
  LogRecordType log_record_type_;

  ItemPointer old_tuple_pos_;

  ItemPointer tuple_pos_;

  eid_t eid_;

  cid_t cid_;
};

class LogRecordFactory {
 public:
  static LogRecord CreateTupleRecord(const LogRecordType log_type,
                                     const ItemPointer &pos, cid_t current_cid,
                                     eid_t current_eid) {
    PL_ASSERT(log_type == LogRecordType::TUPLE_INSERT ||
              log_type == LogRecordType::TUPLE_DELETE ||
              log_type == LogRecordType::TUPLE_UPDATE);
    return LogRecord(log_type, pos, current_eid, current_cid);
  }
};

}  // namespace logging
}  // namespace peloton
