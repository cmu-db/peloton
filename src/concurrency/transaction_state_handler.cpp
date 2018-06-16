#include "concurrency/transaction_state_handler.h"
#include "common/macros.h"
namespace peloton {
namespace concurrency {

  TransactionContext * TransactionStateHandler::ImplicitStart(const size_t thread_id = 0) {
    switch (txn_state_) {
      case TransactionState::IDLE:
        txn_ = txn_manager_.BeginTransaction(thread_id);
        txn_state_ = TransactionState::IMPLICIT_START;
        break;
      case TransactionState::IMPLICIT_START:
      case TransactionState::EXPLICIT_START:
        break;
      case TransactionState::ABORTING:
      case TransactionState::ENDED:
        throw TransactionException("Previous txn not cleanup");
    }

    return txn_;
  }

  TransactionContext * TransactionStateHandler::ExplicitStart(const size_t thread_id = 0) {
    switch (txn_state_) {
      case TransactionState::IDLE:
        txn_ = txn_manager_.BeginTransaction(thread_id);
      case TransactionState ::IMPLICIT_START:
        txn_state_ = TransactionState ::EXPLICIT_START;
        break;
      case TransactionState::ABORTING:
      case TransactionState::ENDED:
        throw TransactionException("txn already started");
    }
  }

  void TransactionStateHandler::ImplicitEnd() {
    switch (txn_state_) {
      case TransactionState::IMPLICIT_START:
        if (txn_->GetResult() == ResultType::FAILURE) {
          txn_manager_.AbortTransaction(txn_);
          txn_state_ = TransactionState::ENDED;
          break;
        }
        auto result = txn_manager_.CommitTransaction(txn_);
        if (result == ResultType::FAILURE) {
          txn_manager_.AbortTransaction(txn_);
          txn_state_ = TransactionState::ENDED;
          break;
        }
        txn_state_ = TransactionState::ENDED;
        break;

      case TransactionState::EXPLICIT_START:
        break;

      case TransactionState::IDLE:
      case TransactionState::ENDED:
        throw TransactionException("Invalid state");
    }
  }

  void TransactionStateHandler::ExplicitCommit() {
    switch (txn_state_) {
      case TransactionState::EXPLICIT_START:
        if (txn_->GetResult() == ResultType::FAILURE) {
          txn_manager_.AbortTransaction(txn_);
          txn_state_ = TransactionState::ABORTING;
          break;
        }

        auto result = txn_manager_.CommitTransaction(txn_);

        if (result == ResultType::FAILURE) {
          txn_manager_.AbortTransaction(txn_);
          txn_state_ = TransactionState::ABORTING;
          break;
        }

        txn_state_ = TransactionState ::ENDED;
        break;

      case TransactionState::IDLE:
        throw TransactionException("No active txn");
      case TransactionState::ENDED:
      case TransactionState::ABORTING:
      case TransactionState::IMPLICIT_START:
        throw TransactionException("Invalid state");

    }
  }

  void TransactionStateHandler::ExplicitAbort() {
    switch (txn_state_) {
      case TransactionState::EXPLICIT_START:
        txn_manager_.AbortTransaction(txn_);
      case TransactionState::ABORTING:
        txn_state_ = TransactionState ::ENDED;
      case TransactionState::IMPLICIT_START:
        throw TransactionException("No active txn");
      case TransactionState::IDLE:
      case TransactionState::ENDED:
        throw TransactionException("Invalid state");
    }
  }

  void TransactionStateHandler::CleanUp() {
    // TODO should we delete the txn here?
    if (txn_state_ == TransactionState::ENDED){
      delete txn_;
      txn_state_ = TransactionState ::IDLE;
    }
  }
}

}