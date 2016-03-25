// //===----------------------------------------------------------------------===//
// //
// //                         PelotonDB
// //
// // transaction_manager.cpp
// //
// // Identification: src/backend/concurrency/pessimistic_transaction_manager.cpp
// //
// // Copyright (c) 2015, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "pessimistic_transaction_manager.h"

// namespace peloton {
// namespace concurrency {
// PessimisticTransactionManager &PessimisticTransactionManager::GetInstance() {
//   static PessimisticTransactionManager txn_manager;
//   return txn_manager;
// }

// // Visibility check
// bool PessimisticTransactionManager::IsVisible(const txn_id_t &tuple_txn_id __attribute__((unused)),
//                                               const cid_t &tuple_begin_cid __attribute__((unused)),
//                                               const cid_t &tuple_end_cid __attribute__((unused))) {
//   return true;
// }

// bool PessimisticTransactionManager::IsOwner(const txn_id_t &tuple_txn_id __attribute__((unused))) {
//   return true;
// }

// bool PessimisticTransactionManager::IsAccessable(const txn_id_t &tuple_txn_id __attribute__((unused)),
//                          const cid_t &tuple_begin_cid __attribute__((unused)),
//                          const cid_t &tuple_end_cid __attribute__((unused))) {
//   return true;
// }

// bool PessimisticTransactionManager::PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   // acquire read lock.
//   current_txn->RecordRead(tile_group_id, tuple_id);
//   return true;
// }

// bool PessimisticTransactionManager::PerformWrite(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   // acquire write lock.
//   current_txn->RecordWrite(tile_group_id, tuple_id);
//   return true;
// }

// bool PessimisticTransactionManager::PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   current_txn->RecordInsert(tile_group_id, tuple_id);
//   return true;
// }

// bool PessimisticTransactionManager::PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   current_txn->RecordDelete(tile_group_id, tuple_id);
//   return true;
// }

// Result PessimisticTransactionManager::CommitTransaction() { 
//   delete current_txn; 
//   current_txn = nullptr;
//   return Result::RESULT_ABORTED;
// }

// Result PessimisticTransactionManager::AbortTransaction() { 
//   delete current_txn; 
//   current_txn = nullptr;
//   return Result::RESULT_ABORTED;
// }
// }
// }