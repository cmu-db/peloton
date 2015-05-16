/*-------------------------------------------------------------------------
 *
 * context.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/context.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"

#include <vector>

namespace nstore {

//===--------------------------------------------------------------------===//
// Context
//===--------------------------------------------------------------------===//

/**
 * Context required by executors at runtime.
 */
class Context {
 public:
  Context(txn_id_t transaction_id, cid_t commit_id)
 : txn_id(transaction_id), local_commit_id(commit_id) {
  }

  inline txn_id_t GetTransactionId() const{
    return txn_id;
  }

  inline cid_t GetCommitId() const{
    return local_commit_id;
  }

  void RecordInsert(const ItemPointer location){
    inserted_slots.push_back(location);
  }

  void RecordDelete(const ItemPointer location){
    deleted_slots.push_back(location);
  }

  bool Commit();

  bool Abort();

 private:

  // transaction id
  txn_id_t txn_id;

  // local commit id
  cid_t local_commit_id;

  // slots mutated by the transaction
  // TODO: (Make it thread safe ?
  std::vector<ItemPointer> inserted_slots;
  std::vector<ItemPointer> deleted_slots;

};

} // End nstore namespace

