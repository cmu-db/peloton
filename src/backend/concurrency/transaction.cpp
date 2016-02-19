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

#include "backend/common/logger.h"
#include "backend/common/platform.h"

#include <chrono>
#include <thread>
#include <iomanip>

namespace peloton {
namespace concurrency {

void Transaction::RecordInsert(ItemPointer location) {
  inserted_tuples[location.block].push_back(location.offset);
}

void Transaction::RecordDelete(ItemPointer location) {
  deleted_tuples[location.block].push_back(location.offset);
}

const std::map<oid_t, std::vector<oid_t>> &Transaction::GetInsertedTuples() {
  return inserted_tuples;
}

const std::map<oid_t, std::vector<oid_t>> &Transaction::GetDeletedTuples() {
  return deleted_tuples;
}

void Transaction::ResetState(void) {
  inserted_tuples.clear();
  deleted_tuples.clear();
}


const std::string Transaction::GetInfo() const{
  std::ostringstream os;

  os << "\tTxn :: @" << this << " ID : " << std::setw(4) << txn_id
     << " Commit ID : " << std::setw(4) << cid
     << " Last Commit ID : " << std::setw(4) << last_cid
     << " Result : " << result_;

  if (next == nullptr) {
    os << " Next : " << std::setw(4) << next->txn_id;
  }

  os << " Ref count : " << std::setw(4) << ref_count << "\n";
  return os.str();
}

}  // End concurrency namespace
}  // End peloton namespace
