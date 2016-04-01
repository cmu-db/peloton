//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.h
//
// Identification: src/backend/concurrency/transaction.h
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
      : txn_id_(INVALID_TXN_ID),
        begin_cid_(INVALID_CID),
        end_cid_(INVALID_CID) {}
        //outer_dep_count_(0),
        //cascading_abort_(false) {}

  Transaction(const txn_id_t &txn_id)
      : txn_id_(txn_id),
        begin_cid_(INVALID_CID),
        end_cid_(INVALID_CID) {}
        //outer_dep_count_(0),
        //cascading_abort_(false) {}

  Transaction(const txn_id_t &txn_id, const cid_t &begin_cid)
      : txn_id_(txn_id),
        begin_cid_(begin_cid),
        end_cid_(INVALID_CID) {}
        //outer_dep_count_(0),
        //cascading_abort_(false) {}

  ~Transaction() {}

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline cid_t GetBeginCommitId() const { return begin_cid_; }

  inline cid_t GetEndCommitId() const { return end_cid_; }

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

  ///////////////////////////////////////////////////////////
  // other transactions can register dependency on me.
  // if A depends on B, then A calls this function when performing read.
  // inline void RegisterDependency(const txn_id_t &txn_id) {
  //   {
  //     std::lock_guard<std::mutex> lock(inner_dep_set_mutex_);
  //     inner_dep_set_.insert(txn_id);
  //   }
  // }

  // other transactions can notify their commit or abort.
  // if A depends on B, then B calls this function before final commitment.
  // inline void NotifyDependency(const bool is_abort) {
  //   if (is_abort == true) {
  //     cascading_abort_ = true;
  //   } else{
  //     --outer_dep_count_;
  //   }
  // }

  // inline std::unordered_set<txn_id_t> &GetChildTxnIds() const {
  //   return inner_dep_set_;
  // }

  /////////////////////////////////////////////////////////////
  // this function is called by myself.
  // inline bool CheckDependency(const txn_id_t &txn_id) {
  //   // if this dependency has not been registered before.
  //   // if A depends on B and A has already registered its dependency on B,
  //   // then return false.
  //   return outer_dep_set_.find(txn_id) == outer_dep_set_.end();
  // }

  // this function is called by myself.
  // inline void RecordDependency(const txn_id_t &txn_id) {
  //   // if this dependency has not been registered before.
  //   assert(outer_dep_set_.find(txn_id) == outer_dep_set_.end());
  //   outer_dep_set_.insert(txn_id);
  //   ++outer_dep_count_;
  // }

  // this function is called by myself.
  // inline bool IsCommittable() {
  //   return outer_dep_count_ == 0;
  // }

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

  std::map<oid_t, std::map<oid_t, RWType>> rw_set_;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;

  // //================================
  // // the following structures are for building transaction dependency graph.
  // std::mutex inner_dep_set_mutex_;

  // // transactions that are dependent on me.
  // // other transactions can register their dependencies on me through txn manager.
  // std::unordered_set<txn_id_t> inner_dep_set_; 
  
  // // transactions that i am dependent on.
  // // this structure is manipulated by myself.
  // std::unordered_set<txn_id_t> outer_dep_set_; 
  
  // std::atomic<size_t> outer_dep_count_;
  // std::atomic<bool> cascading_abort_;
};

}  // End concurrency namespace
}  // End peloton namespace
