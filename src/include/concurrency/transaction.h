//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.h
//
// Identification: src/include/concurrency/transaction.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <atomic>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "common/printable.h"
#include "common/types.h"
#include "common/exception.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

enum RWType {
  RW_TYPE_READ,
  RW_TYPE_UPDATE,
  RW_TYPE_INSERT,
  RW_TYPE_DELETE,
  RW_TYPE_INS_DEL  // delete after insert.
};

class Transaction : public Printable {
  Transaction(Transaction const &) = delete;

 public:
  Transaction()
      : txn_id_(INVALID_TXN_ID),
        begin_cid_(INVALID_CID),
        end_cid_(MAX_CID),
        is_written_(false),
        insert_count_(0) {}

  Transaction(const txn_id_t &txn_id)
      : txn_id_(txn_id),
        begin_cid_(INVALID_CID),
        end_cid_(MAX_CID),
        is_written_(false),
        insert_count_(0) {}

  Transaction(const txn_id_t &txn_id, const cid_t &begin_cid)
      : txn_id_(txn_id),
        begin_cid_(begin_cid),
        end_cid_(MAX_CID),
        is_written_(false),
        insert_count_(0) {}

  ~Transaction() {}

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline cid_t GetBeginCommitId() const { return begin_cid_; }

  inline cid_t GetEndCommitId() const { return end_cid_; }

  inline size_t GetEpochId() const { return epoch_id_; }

  inline void SetEndCommitId(cid_t eid) { end_cid_ = eid; }

  inline void SetEpochId(const size_t eid) { epoch_id_ = eid; }

  void RecordRead(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  // Return true if we detect INS_DEL
  bool RecordDelete(const ItemPointer &);

  const std::map<oid_t, std::map<oid_t, RWType>> &GetRWSet();

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(Result result) { result_ = result; }

  // Get result and status
  inline Result GetResult() const { return result_; }

  inline bool IsReadOnly() const {
    return is_written_ == false && insert_count_ == 0;
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id_;

  // start commit id
  cid_t begin_cid_;

  // end commit id
  cid_t end_cid_;

  // epoch id
  size_t epoch_id_;

  std::map<oid_t, std::map<oid_t, RWType>> rw_set_;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;

  bool is_written_;
  size_t insert_count_;
};

}  // End concurrency namespace
}  // End peloton namespace
