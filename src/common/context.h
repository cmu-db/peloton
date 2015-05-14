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

  Context(txn_id_t transaction_id)
 : txn_id(transaction_id) {
  }

  txn_id_t GetTransactionId() const{
    return txn_id;
  }

  void TrackInsert(const ItemPointer location){
    inserted_slots.push_back(location);
  }

  void TrackDelete(const ItemPointer location){
    deleted_slots.push_back(location);
  }

  void Undo(){
    // TODO: Need an undo mechanism here
  }

 private:

  txn_id_t txn_id;

  // slots mutated by the transaction
  std::vector<ItemPointer> inserted_slots;
  std::vector<ItemPointer> deleted_slots;

};

} // End nstore namespace

