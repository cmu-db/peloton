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

#include <mutex>
#include <atomic>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "backend/common/printable.h"
#include "backend/common/types.h"
#include "backend/common/exception.h"

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
      : txn_id(INVALID_TXN_ID),
        begin_cid(INVALID_CID),
        end_cid(INVALID_CID),
        outer_dep_count(0),
        cascading_abort(false) {}

  Transaction(const txn_id_t &txn_id, const cid_t &begin_cid)
      : txn_id(txn_id),
        begin_cid(begin_cid),
        end_cid(INVALID_CID),
        outer_dep_count(0),
        cascading_abort(false) {}

  ~Transaction() {}

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id; }

  inline cid_t GetBeginCommitId() const { return begin_cid; }

  inline cid_t GetEndCommitId() const { return end_cid; }

  // record read set
  void RecordRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  // record write set
  void RecordUpdate(const oid_t &tile_group_id, const oid_t &tuple_id);

  // record insert set
  void RecordInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  // record delete set
  void RecordDelete(const oid_t &tile_group_id, const oid_t &tuple_id);

  void RecordRead(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  void RecordDelete(const ItemPointer &);

  const std::map<oid_t, std::map<oid_t, RWType>> &GetRWSet();

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(Result result) { result_ = result; }

  // Get result and status
  inline Result GetResult() const { return result_; }

  // other transactions can register dependency on me.
  inline void RegisterDependency(const txn_id_t &txn_id) {
    {
      std::lock_guard<std::mutex> lock(inner_dep_set_mutex);
      inner_dep_set.insert(txn_id);
    }
  }

  // other transactions can deregister their dependency.
  inline void ReleaseDependency(const bool is_abort) {
    if (is_abort == true) {
      cascading_abort = true;
    } else{
      --outer_dep_count;
    }
  }

  // this function is called by myself.
  inline bool CheckDependency(const txn_id_t &txn_id) {
    // if this dependency has not been registered before.
    return outer_dep_set.find(txn_id) == outer_dep_set.end();
  }

  // this function is called by myself.
  inline void RecordDependency(const txn_id_t &txn_id) {
    // if this dependency has not been registered before.
    assert(outer_dep_set.find(txn_id) == outer_dep_set.end());
    outer_dep_set.insert(txn_id);
    ++outer_dep_count;
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id;

  // start commit id
  cid_t begin_cid;

  // end commit id
  cid_t end_cid;

  std::map<oid_t, std::map<oid_t, RWType>> rw_set;

  std::mutex inner_dep_set_mutex;
  std::unordered_set<txn_id_t> inner_dep_set; // transactions that are dependent on me.

  std::unordered_set<txn_id_t> outer_dep_set; // transactions that i am dependent on.
  std::atomic<size_t> outer_dep_count;
  std::atomic<bool> cascading_abort;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;
};

}  // End concurrency namespace
}  // End peloton namespace
