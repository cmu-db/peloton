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
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/exception.h"
#include "common/item_pointer.h"
#include "common/printable.h"
#include "type/types.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

class Transaction : public Printable {
  Transaction(Transaction const &) = delete;

 public:
  Transaction(const size_t thread_id,
              const IsolationLevelType isolation,
              const cid_t &read_id) {
    Init(thread_id, isolation, read_id);
  }

  Transaction(const size_t thread_id,
              const IsolationLevelType isolation,
              const cid_t &read_id, 
              const cid_t &commit_id) {
    Init(thread_id, isolation, read_id, commit_id);
  }

  ~Transaction() {}

 private:

  void Init(const size_t thread_id, 
            const IsolationLevelType isolation, 
            const cid_t &read_id) {
    Init(thread_id, isolation, read_id, read_id);
  }

  void Init(const size_t thread_id, 
            const IsolationLevelType isolation, 
            const cid_t &read_id, 
            const cid_t &commit_id) {
    // initially, all the three ids are set to read_id.
    txn_id_ = read_id;
    
    read_id_ = read_id;

    // commit id can be set at a transaction's commit phase.
    commit_id_ = commit_id;

    epoch_id_ = read_id_ >> 32;

    thread_id_ = thread_id;

    isolation_level_ = isolation;

    is_written_ = false;
    
    insert_count_ = 0;
    
    gc_set_.reset(new GCSet());
  }


 public:
  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline size_t GetThreadId() const { return thread_id_; }

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline cid_t GetReadId() const { return read_id_; }

  inline cid_t GetCommitId() const { return commit_id_; }

  inline eid_t GetEpochId() const { return epoch_id_; }

  inline void SetCommitId(const cid_t commit_id) { commit_id_ = commit_id; }

  void RecordRead(const ItemPointer &);

  void RecordReadOwn(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  // Return true if we detect INS_DEL
  bool RecordDelete(const ItemPointer &);

  RWType GetRWType(const ItemPointer &);

  bool IsInRWSet(const ItemPointer &location) {

    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;

    if (rw_set_.find(tile_group_id) != rw_set_.end() &&
        rw_set_.at(tile_group_id).find(tuple_id) !=
            rw_set_.at(tile_group_id).end()) {
      return true;
    } else {
      return false;
    }
  }

  inline const ReadWriteSet &GetReadWriteSet() { return rw_set_; }

  inline std::shared_ptr<GCSet> GetGCSetPtr() {
    return gc_set_;
  }

  inline bool IsGCSetEmpty() { return gc_set_->size() == 0; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(ResultType result) { result_ = result; }

  // Get result and status
  inline ResultType GetResult() const { return result_; }

  inline bool IsReadOnly() const {
    return is_written_ == false && insert_count_ == 0;
  }

  inline IsolationLevelType GetIsolationLevel() const {
    return isolation_level_;
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id_;

  // thread id
  size_t thread_id_;

  // read id
  // this id determines which tuple versions the transaction can access.
  cid_t read_id_;

  // commit id
  // this id determines the id attached to the tuple version written by the transaction.
  cid_t commit_id_;

  // epoch id can be extracted from read id.
  // GC manager uses this id to check whether a version is still visible.
  eid_t epoch_id_;

  ReadWriteSet rw_set_;

  // this set contains data location that needs to be gc'd in the transaction.
  std::shared_ptr<GCSet> gc_set_;

  // result of the transaction
  ResultType result_ = ResultType::SUCCESS;

  bool is_written_;
  size_t insert_count_;

  IsolationLevelType isolation_level_;

};

}  // End concurrency namespace
}  // End peloton namespace
