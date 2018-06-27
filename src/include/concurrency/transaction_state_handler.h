#pragma once

#include "transaction_manager_factory.h"

namespace peloton {
namespace concurrency {
  enum class TransactionState{
   IDLE = 0,
   IMPLICIT_START,
    EXPLICIT_START,
    ABORTING,
    ENDED,
 };

class TransactionStateHandler {
 public:
  TransactionContext *ImplicitStart(const size_t thread_id = 0);

  TransactionContext * ExplicitStart(const size_t thread_id = 0);

  void ImplicitEnd();

  void ExplicitCommit();

  void ExplicitAbort();

  void CleanUp();

  inline TransactionContext *GetCurrentTxn() {
    return txn_;
  }

  inline TransactionState GetCurrentTxnState() {
    return txn_state_;
  }

 private:
  TransactionState txn_state_;
  TransactionContext *txn_;
  TransactionManager &txn_manager_ = TransactionManagerFactory::GetInstance();

};

}
}