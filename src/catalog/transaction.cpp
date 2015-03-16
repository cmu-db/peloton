/*-------------------------------------------------------------------------
 *
 * transaction.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/transaction.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/transaction.h"
#include "common/synch.h"

#include <chrono>
#include <thread>
#include <iomanip>

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

void Transaction::RecordInsert(const storage::TileGroup* tile, id_t offset) {
  {
    std::lock_guard<std::mutex> insert_lock(txn_mutex);

    inserted_tuples[tile].push_back(offset);
  }
}

void Transaction::RecordDelete(const storage::TileGroup* tile, id_t offset) {
  {
    std::lock_guard<std::mutex> delete_lock(txn_mutex);

    deleted_tuples[tile].push_back(offset);
  }
}

bool Transaction::HasInsertedTuples(const storage::TileGroup* tile) const {
  auto tile_itr = inserted_tuples.find(tile);

  if(tile_itr != inserted_tuples.end() && !tile_itr->second.empty())
    return true;

  return false;
}

bool Transaction::HasDeletedTuples(const storage::TileGroup* tile) const {
  auto tile_itr = deleted_tuples.find(tile);

  if(tile_itr != deleted_tuples.end() && !tile_itr->second.empty())
    return true;

  return false;
}

const std::map<const storage::TileGroup*, std::vector<id_t> >& Transaction::GetInsertedTuples() {
  return inserted_tuples;
}

const std::map<const storage::TileGroup*, std::vector<id_t> >& Transaction::GetDeletedTuples() {
  return deleted_tuples;
}

void Transaction::IncrementRefCount() {
  ++ref_count;
}

void Transaction::DecrementRefCount() {
  assert(ref_count > 0);

  // drop txn when ref count reaches 1
  if (ref_count.fetch_sub(1) == 1) {
    delete this;
  }
}

std::ostream& operator<<(std::ostream& os, const Transaction& txn) {

  os << "\tTxn :: @" <<  &txn << " ID : " << std::setw(4) << txn.txn_id
      << " Commit ID : " << std::setw(4) << txn.cid
      << " Last Commit ID : " << std::setw(4) << txn.last_cid;

  if(txn.next == nullptr) {
    os << " Next : " << std::setw(4) << txn.next;
  }
  else {
    os << " Next : " << std::setw(4) << txn.next->txn_id;
  }

  os << " Ref count : " << std::setw(4) << txn.ref_count <<  "\n";

  return os;
}

//===--------------------------------------------------------------------===//
// Transaction Manager
//===--------------------------------------------------------------------===//

// Get entry in table
Transaction *TransactionManager::GetTransaction(txn_id_t txn_id) {
  if(txn_table.count(txn_id) != 0) {
    return txn_table.at(txn_id);
  }

  return nullptr;
}

// Begin a new transaction
Transaction *TransactionManager::BeginTransaction(){
  Transaction *next_txn = new Transaction(GetNextTransactionId(), GetLastCommitId());
  return next_txn;
}


std::vector<Transaction *> TransactionManager::GetCurrentTransactions(){
  std::vector<Transaction *> txns;

  for(auto entry : txn_table)
    txns.push_back(entry.second);

  return txns;
}

bool TransactionManager::IsValid(txn_id_t txn_id){
  return (txn_id < next_txn_id);
}

void TransactionManager::EndTransaction(Transaction *txn, bool sync){
  // XXX LOG :: commit entry
  if(sync) {
  }

  // erase entry in transaction table
  txn_table.erase(txn->txn_id);
}

//===--------------------------------------------------------------------===//
// Commit Processing
//===--------------------------------------------------------------------===//

void TransactionManager::BeginCommitPhase(Transaction *txn){

  // successor in the transaction list will point to us
  txn->IncrementRefCount();

  while (true) {

    // try to append to the pending transaction list
    if (atomic_cas<Transaction *>(&last_txn->next, nullptr, txn)) {

      // the last transaction pointer also points to us
      txn->IncrementRefCount();

      // assign cid to the txn
      txn->cid = last_txn->cid + 1;

      auto tmp = last_txn;
      last_txn = txn;

      // drop a reference to previous last transaction pointer
      tmp->DecrementRefCount();

      return;
    }

    // sleep for some time
    std::chrono::milliseconds sleep_time(1);
    std::this_thread::sleep_for(sleep_time);
  }

}

void TransactionManager::CommitModifications(Transaction *txn, bool sync){

  // commit deletes by setting tuple's end commit id
  for(auto entry : txn->GetDeletedTuples()) {
    const storage::TileGroup *tile_group = entry.first;
    storage::TileGroupHeader *header = tile_group->GetHeader();

    for(auto tuple_slot : entry.second)
      header->SetEndCommitId(tuple_slot, txn->cid);
  }

  // commit inserts by setting tuple's begin commit id
  for(auto entry : txn->GetInsertedTuples()) {
    const storage::TileGroup *tile_group = entry.first;
    storage::TileGroupHeader *header = tile_group->GetHeader();

    for(auto tuple_slot : entry.second)
      header->SetBeginCommitId(tuple_slot, txn->cid);
  }

  // XXX LOG :: commit entry
  if(sync) {
  }
}

void TransactionManager::CommitPendingTransactions(std::vector<Transaction *>& pending_txns, Transaction *txn){

  // add ourself to the list
  pending_txns.push_back(txn);

  auto current_txn = txn->next;

  // commit all pending transactions
  while (current_txn != nullptr && current_txn->waiting_to_commit == true) {

    // try to increment last finished cid
    if (atomic_cas(&last_cid, current_txn->cid - 1, current_txn->cid)) {

      // if that worked, add transaction to list
      pending_txns.push_back(current_txn);

      std::cout << "Pending Txn  : " << current_txn->txn_id << "\n";

      current_txn = current_txn->next;
      continue;
    } else {

      // it did not work, so some other txn must have squeezed in
      // so, stop processing commit dependencies

      break;
    }

  }

}

std::vector<Transaction*> TransactionManager::EndCommitPhase(Transaction * txn, bool sync){
  std::vector<Transaction *> txn_list;

  // try to increment last commit id
  if (atomic_cas(&last_cid, txn->cid - 1, txn->cid)) {

    std::cout << "update lcid worked : " << txn->txn_id << "\n";

    // if that worked, commit all pending transactions
    CommitPendingTransactions(txn_list, txn);

  } else {

    std::cout << "add to wait list : " << txn->txn_id << "\n";

    // it did not work, so add to waiting list
    // some other transaction with lower commit id will commit us later
    txn->waiting_to_commit = true;

    // make sure that the transaction we are waiting for has not finished
    // before we could add ourselves to the list of pending transactions
    // we try incrementing the last finished cid again
    if (atomic_cas(&last_cid, txn->cid - 1, txn->cid)) {

      // it worked on the second try
      txn->waiting_to_commit = false;

      CommitPendingTransactions(txn_list, txn);
    }
  }

  // clear txn entry in txn table
  EndTransaction(txn, sync);

  return txn_list;
}


void TransactionManager::CommitTransaction(Transaction *txn, bool sync){
  assert(txn != nullptr);

  // validate txn id
  if (!IsValid(txn->txn_id)) {
    throw TransactionException("Transaction not found in transaction table : " + std::to_string(txn->txn_id));
  }

  // begin commit phase : get cid and add to transaction list
  BeginCommitPhase(txn);

  // commit all modifications
  CommitModifications(txn, sync);

  // end commit phase : increment last_cid and process pending txns if needed
  std::vector<Transaction *> committed_txns = EndCommitPhase(txn, sync);

  // XXX LOG : group commit entry

  // process all committed txns
  for (auto committed_txn : committed_txns)
    committed_txn->DecrementRefCount();

}

//===--------------------------------------------------------------------===//
// Abort Processing
//===--------------------------------------------------------------------===//

void TransactionManager::WaitForCurrentTransactions() const{

  std::vector<txn_id_t> current_txns;

  // record all currently running transactions
  for(auto entry : txn_table)
    current_txns.push_back(entry.first);


  // block until all current txns are finished
  while (true) {

    // remove all finished txns from list
    for(auto txn_id : current_txns) {
      if(txn_table.count(txn_id) == 0) {
        auto location = std::find(current_txns.begin(), current_txns.end(), txn_id);
        if (location != current_txns.end())
          current_txns.erase(location);
      }
    }

    // all transactions in waiting list finished ?
    if(current_txns.empty())
      break;

    // sleep for some time
    std::chrono::milliseconds sleep_time(10);
    std::this_thread::sleep_for(sleep_time);
  }

}

void TransactionManager::AbortTransaction(Transaction *txn){

  // rollback deletes by resetting tuple's end commit id
  for(auto entry : txn->GetDeletedTuples()) {
    const storage::TileGroup *tile_group = entry.first;
    storage::TileGroupHeader *header = tile_group->GetHeader();

    for(auto tuple_slot : entry.second)
      header->SetEndCommitId(tuple_slot, MAX_CID);
  }

  // rollback inserts by resetting tuple's txn id

  EndTransaction(txn, false);

  // XXX LOG :: abort entry
}


} // End catalog namespace
} // End nstore namespace
