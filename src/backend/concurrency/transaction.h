//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction.h
//
// Identification: src/backend/concurrency/transaction.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/common/exception.h"
#include "backend/storage/tile_group.h"
#include "backend/concurrency/transaction_manager.h"

#include <atomic>
#include <cassert>
#include <vector>
#include <map>

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

class Transaction {
  friend class TransactionManager;

  Transaction(Transaction const &) = delete;

 public:
  Transaction()
      : txn_id(INVALID_TXN_ID),
        cid(INVALID_CID),
        last_cid(INVALID_CID),
        ref_count(BASE_REF_COUNT),
        waiting_to_commit(false),
        next(nullptr) {}

  Transaction(txn_id_t txn_id, cid_t last_cid)
      : txn_id(txn_id),
        cid(INVALID_CID),
        last_cid(last_cid),
        ref_count(BASE_REF_COUNT),
        waiting_to_commit(false),
        next(nullptr) {}

  ~Transaction() {
    if (next != nullptr) {
      next->DecrementRefCount();
    }
  }

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id; }

  inline cid_t GetCommitId() const { return cid; }

  inline cid_t GetLastCommitId() const { return last_cid; }

  // record inserted tuple
  void RecordInsert(ItemPointer location);

  // record deleted tuple
  void RecordDelete(ItemPointer location);

  // check if it has inserted any tuples in given tile group
  bool HasInsertedTuples(storage::TileGroup *tile_group) const;

  // check if it has deleted any tuples in given tile group
  bool HasDeletedTuples(storage::TileGroup *tile_group) const;

  const std::map<storage::TileGroup *, std::vector<oid_t>> &GetInsertedTuples();

  const std::map<storage::TileGroup *, std::vector<oid_t>> &GetDeletedTuples();

  // reset inserted tuples and deleted tuples
  // used by recovery (logging)
  void ResetStates(void);

  // maintain reference counts for transactions
  void IncrementRefCount();

  void DecrementRefCount();

  // Get a string representation of this txn
  friend std::ostream &operator<<(std::ostream &os, const Transaction &txn);

  // Set result and status
  void SetResult(Result result);

  // Get result and status
  Result GetResult() const;

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id;

  // commit id
  cid_t cid;

  // last visible commit id
  cid_t last_cid;

  // references
  std::atomic<size_t> ref_count;

  // waiting for commit ?
  std::atomic<bool> waiting_to_commit;

  // cid context
  Transaction *next __attribute__((aligned(16)));

  // inserted tuples
  std::map<storage::TileGroup *, std::vector<oid_t>> inserted_tuples;

  // deleted tuples
  std::map<storage::TileGroup *, std::vector<oid_t>> deleted_tuples;

  // synch helpers
  std::mutex txn_mutex;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;
};

}  // End concurrency namespace
}  // End peloton namespace
