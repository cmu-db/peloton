//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction.cpp
//
// Identification: src/backend/concurrency/transaction.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/concurrency/transaction.h"

#include "backend/common/synch.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"

#include <chrono>
#include <thread>
#include <iomanip>

namespace peloton {
namespace concurrency {

void Transaction::RecordInsert(ItemPointer location) {
  auto &manager = catalog::Manager::GetInstance();
  inserted_tuples[location.block].push_back(location.offset);
}

void Transaction::RecordDelete(ItemPointer location) {
  auto &manager = catalog::Manager::GetInstance();
  deleted_tuples[location.block].push_back(location.offset);
}

const std::map<oid_t, std::vector<oid_t>> &
Transaction::GetInsertedTuples() {
  return inserted_tuples;
}

const std::map<oid_t, std::vector<oid_t>> &
Transaction::GetDeletedTuples() {
  return deleted_tuples;
}

void Transaction::ResetState(void) {
  inserted_tuples.clear();
  deleted_tuples.clear();
}

std::ostream &operator<<(std::ostream &os, const Transaction &txn) {
  os << "\tTxn :: @" << &txn << " ID : " << std::setw(4) << txn.txn_id
     << " Commit ID : " << std::setw(4) << txn.cid
     << " Last Commit ID : " << std::setw(4) << txn.last_cid
     << " Result : " << txn.result_;

  if (txn.next == nullptr) {
    os << " Next : " << std::setw(4) << txn.next;
  } else {
    os << " Next : " << std::setw(4) << txn.next->txn_id;
  }

  os << " Ref count : " << std::setw(4) << txn.ref_count << "\n";
  return os;
}

}  // End concurrency namespace
}  // End peloton namespace
