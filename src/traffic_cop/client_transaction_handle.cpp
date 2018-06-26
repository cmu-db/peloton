//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// client_transaction_handle.cpp
//
// Identification: src/traffic_cop/client_transaction_handle.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "traffic_cop/client_transaction_handle.h"
#include <memory>
namespace peloton {
namespace tcop {

  /* Function implementations of SingleStmtClientTxnHandler */
  TxnContext *SingleStmtClientTxnHandler::ImplicitBegin(const size_t thread_id, ClientTxnHandle &handle) {
    switch (handle.txn_state_) {
      case TransactionState::IDLE: {
        handle.txn_ = TxnManagerFactory::GetInstance().BeginTransaction(thread_id);
        handle.txn_state_ =TransactionState::STARTED;
      }
      case TransactionState::STARTED:
      case TransactionState::FAILING:
      case TransactionState::ABORTING:
        break;
    }
    return handle.txn_;
  }

  void SingleStmtClientTxnHandler::End(ClientTxnHandle &handle) {
    // TODO Implement this function
  }

  void SingleStmtClientTxnHandler::Abort(ClientTxnHandle &handle) {
    // TODO Implement this function
  }


  /* Function implementations of MultiStmtsClientTxnHandler */
  TxnContext *MultiStmtsClientTxnHandler::ImplicitBegin(const size_t, ClientTxnHandle &handle_) {
    return handle_.GetTxn();
  }

  TxnContext *MultiStmtsClientTxnHandler::ExplicitBegin(const size_t thread_id = 0, ClientTxnHandle & handle){
    switch (handle.txn_state_) {
      case TransactionState::IDLE: {
        handle.txn_ = TxnManagerFactory::GetInstance().BeginTransaction(thread_id);
        handle.txn_state_ = TransactionState::STARTED;
      }
      case TransactionState::STARTED:
        TxnManagerFactory::GetInstance().AbortTransaction(handle.txn_);
        handle.txn_state_ = TransactionState::ABORTING;
        throw TransactionException("Current Transaction started already");
      case TransactionState::FAILING:
      case TransactionState::ABORTING:
        break;
    }
    return handle.txn_;
  }

  bool MultiStmtsClientTxnHandler::Commit(ClientTxnHandle &handler) {
    // TODO implement this function
    return false;
  }

  void MultiStmtsClientTxnHandler::Abort(ClientTxnHandle &handler) {
    // TODO implement this function
  }

  /* Function implementations of ClientTxnHandle */
  TxnContext *ClientTxnHandle::ImplicitBegin(const size_t thread_id) {
    return handler_->ImplicitBegin(thread_id, *this);
  }

  TxnContext *ClientTxnHandle::ExplicitBegin(const size_t thread_id) {
    if (single_stmt_handler_) {
      ChangeToMultiStmtsHandler();
    }
    return handler_->ExplicitBegin(thread_id, *this);
  }

  void ClientTxnHandle::ImplicitEnd() {
    handler_->End(*this);
    if (txn_state_ == TransactionState::IDLE && !single_stmt_handler_) {
      ChangeToSingleStmtHandler();
    }
  }

  void ClientTxnHandle::ExplicitAbort() {
    handler_->Abort(*this);
    if (!single_stmt_handler_)
      ChangeToSingleStmtHandler();
  }

  bool ClientTxnHandle::ExplicitCommit() {
    bool success = handler_->Commit(*this);
    if (success && !single_stmt_handler_) {
      ChangeToSingleStmtHandler();
    }
    return success;
  }

}
}