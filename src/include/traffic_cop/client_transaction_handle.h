//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// client_transaction_handle.h
//
// Identification: src/include/traffic_cop/client_transaction_handle.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_manager_factory.h"
namespace peloton {
namespace tcop {

using TxnContext = concurrency::TransactionContext;
using TxnManagerFactory = concurrency::TransactionManagerFactory;

enum class TransactionState{
  IDLE = 0,
  STARTED,
  FAILING,
  ABORTING,
};

class ClientTxnHandle;

/**
 * abtract class to provide a unified interface of txn handling
 */
class AbtractClientTxnHandler {
 public:
  /**
   * @brief Start a txn if there is no txn at the moment this function is called.
   * @param thread_id number to generate epoch id in a distributed manner
   * @param handle Client transaction context
   * @return Current trancation that is started
   * @throw TransactionException when no txn can be started (eg. current txn is failed already)
   */
  virtual TxnContext *ImplicitBegin(const size_t thread_id = 0, ClientTxnHandle &handle) = 0;

  /**
   * @brief Force starting a txn
   * @param thread_id number to generate epoch id in a distributed manner
   * @param handle Client transaction context
   * @return Current trancation that is started
   * @throw TransactionException when no txn can be started (eg. there is already an txn)
   */
  virtual TxnContext *ExplicitBegin(const size_t thread_id = 0, ClientTxnHandle &handle) = 0;

  /**
   * @brief Implicitly end a txn
   * @param handle
   * @param handle Client transaction context
   */
  virtual void End(ClientTxnHandle &handle) = 0;

  /**
   * @brief Explicitly commit a txn
   * @param handle Client transaction context
   * @throw TransactionException when there is no txn started
   */
  virtual bool Commit(ClientTxnHandle &handle) = 0;

  /**
   * @brief Explicitly abort a txn
   * @param handle Client transaction context
   */
  virtual void Abort(ClientTxnHandle &handle) = 0;

};

/**
 * Client Transaction handler for Transaction Handler when in Single-Statement Mode
 */
class SingleStmtClientTxnHandler : AbtractClientTxnHandler{

  /**
   * @see AbstractClientTxnHandler
   */
  TxnContext *ImplicitBegin(const size_t thread_id = 0, ClientTxnHandle &handle);

  /**
   * @brief This function should never be called in this mode
   */
  inline TxnContext *ExplicitBegin(const size_t, ClientTxnHandle &) {
    throw TransactionException("Should not be called");
  }

  /**
   * @see AbstractClientTxnHandler
   */
  void End(ClientTxnHandle &handle);

  /**
   * @brief This function should never be called in this mode
   */
  inline bool Commit(ClientTxnHandle &handle) {
    throw TransactionException("Should not be called");
  }

  /**
   * @see AbstractClientTxnHandler
   */
  void Abort(ClientTxnHandle &handle);

};

class MultiStmtsClientTxnHandler : AbtractClientTxnHandler {

  /**
   * @see AbstractClientTxnHandler
   */
  TxnContext *ImplicitBegin(const size_t, ClientTxnHandle &handle_);

  /**
   * @see AbstractClientTxnHandler
   */
  TxnContext *ExplicitBegin(const size_t thread_id = 0, ClientTxnHandle & handle);

  /**
   * @see AbstractClientTxnHandler
   */
  inline void End(ClientTxnHandle &handle) {}

  /**
   * @see AbstractClientTxnHandler
   */
  bool Commit(ClientTxnHandle &handle);

  /**
   * @see AbstractClientTxnHandler
   */
  void Abort(ClientTxnHandle &handle);
};

/**
 * @brief Wrapper class that could provide functions to properly start and end a transaction.
 *
 * It would operate in either Single-Statement or Multi-Statements mode, using different handler
 *
 */
class ClientTxnHandle {
  friend class AbtractClientTxnHandler;
  friend class SingleStmtClientTxnHandler;
  friend class MultiStmtsClientTxnHandler;

 public:

  /**
   * Start a transaction if there is no transaction
   * @param thread_id number to generate epoch id in a distributed manner
   * @return transaction context
   */
  TxnContext *ImplicitBegin(const size_t thread_id = 0);

  /**
   * Force starting a transaction if there is no transaction
   * @param thread_id number to generate epoch id in a distributed manner
   * @return transaction context
   */
  TxnContext *ExplicitBegin(const size_t thread_id = 0);

  /**
   * Commit/Abort a transaction and do the necessary cleanup
   */
  void ImplicitEnd();

  /**
   * Explicitly commit a transaction
   * @return if the commit is successful
   */
  bool ExplicitCommit();

  /**
   * Explicitly abort a transaction
   */
  void ExplicitAbort();

  /**
   * @brief Getter function of txn state
   * @return current trancation state
   */
  inline TransactionState GetTxnState() {
    return txn_state_;
  }

  /**
   * @brief Getter function of current transaction context
   * @return current transaction context
   */
  inline TxnContext *GetTxn() {
    return txn_;
  }

 private:

  TransactionState txn_state_;

  TxnContext *txn_;

  bool single_stmt_handler_ = true;

  std::unique_ptr<AbtractClientTxnHandler> handler_;

  inline void ChangeToSingleStmtHandler() {
    handler_ = std::unique_ptr<SingleStmtClientTxnHandler>(new SingleStmtClientTxnHandler());
    single_stmt_handler_ = true;
  }

  inline void ChangeToMultiStmtsHandler() {
    handler_ = std::unique_ptr<MultiStmtsClientTxnHandler>(new MultiStmtsClientTxnHandler());
    single_stmt_handler_ = false;
  }
};

}
}