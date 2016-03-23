// //===----------------------------------------------------------------------===//
// //
// //                         PelotonDB
// //
// // transaction_manager.cpp
// //
// // Identification: src/backend/concurrency/empty_transaction_manager.cpp
// //
// // Copyright (c) 2015, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "empty_transaction_manager.h"

// namespace peloton {
// namespace concurrency {
// EmptyTransactionManager &EmptyTransactionManager::GetInstance() {
//   static EmptyTransactionManager txn_manager;
//   return txn_manager;
// }

// // Visibility check
// bool EmptyTransactionManager::IsVisible(const txn_id_t &tuple_txn_id __attribute__((unused)),
//                                               const cid_t &tuple_begin_cid __attribute__((unused)),
//                                               const cid_t &tuple_end_cid __attribute__((unused))) {
//   return true;
// }

// void EmptyTransactionManager::CommitTransaction() { delete current_txn; }

// void EmptyTransactionManager::AbortTransaction() { delete current_txn; }
// }
// }