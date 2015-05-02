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

 private:

  txn_id_t txn_id;

};

} // End nstore namespace

