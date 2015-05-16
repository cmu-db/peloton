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
 : txn_id(transaction_id), commit_id(commit_id) {
  }

  txn_id_t GetTransactionId() const{
    return txn_id;
  }

  cid_t GetCommitId() const{
    return commit_id;
  }

  void RecordInsert(const ItemPointer location){
    inserted_slots.push_back(location);
  }

  void RecordDelete(const ItemPointer location){
    deleted_slots.push_back(location);
  }

  bool Commit(){
    // TODO: Need a commit mechanism
    return true;
  }

  bool Abort(){
    // TODO: Need an abort mechanism
    return true;
  }

 private:

  txn_id_t txn_id;
  cid_t commit_id;

  // slots mutated by the transaction
  std::vector<ItemPointer> inserted_slots;
  std::vector<ItemPointer> deleted_slots;

};

} // End nstore namespace

