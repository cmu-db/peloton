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

#include <atomic>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "common/printable.h"
#include "type/types.h"
#include "common/exception.h"


namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//


class Transaction : public Printable {
  Transaction(Transaction const &) = delete;

 public:
  Transaction() {
    Init(INVALID_TXN_ID, INVALID_CID);
  }

  Transaction(const txn_id_t &txn_id) {
    Init(txn_id, INVALID_CID);
  }

  Transaction(const txn_id_t &txn_id, const cid_t &begin_cid) {
    Init(txn_id, begin_cid);
  }

  Transaction(const txn_id_t &txn_id, const cid_t &begin_cid, bool ro) {
    Init(txn_id, begin_cid);
    declared_readonly_ = ro;
  }

  ~Transaction() {}

  void Init(const txn_id_t &txn_id, const cid_t &begin_cid) {
    txn_id_ = txn_id;
    begin_cid_ = begin_cid;
    end_cid_ = MAX_CID;
    is_written_ = false;
    declared_readonly_ = false;
    insert_count_ = 0;
    gc_set_.reset(new ReadWriteSet());
  }

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

  void RecordReadOwn(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  // Return true if we detect INS_DEL
  bool RecordDelete(const ItemPointer &);

  RWType GetRWType(const ItemPointer&);

  inline const ReadWriteSet &GetReadWriteSet() {
    return rw_set_;
  }

  inline std::shared_ptr<ReadWriteSet> GetGCSetPtr() {
    return gc_set_;
  }

  inline bool IsGCSetEmpty() {
    return gc_set_->size() == 0;
  }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(ResultType result) { result_ = result; }

  // Get result and status
  inline ResultType GetResult() const { return result_; }

  inline bool IsReadOnly() const {
    return is_written_ == false && insert_count_ == 0;
  }

  inline bool IsDeclaredReadOnly() const {
    return declared_readonly_;
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

  ReadWriteSet rw_set_;

  // this set contains data location that needs to be gc'd in the transaction.
  std::shared_ptr<ReadWriteSet> gc_set_;

  // result of the transaction
  ResultType result_ = peloton::ResultType::RESULT_TYPE_SUCCESS;

  bool is_written_;
  size_t insert_count_;

  bool declared_readonly_;
};

}  // End concurrency namespace
}  // End peloton namespace
