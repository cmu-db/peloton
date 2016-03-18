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

void Transaction::RecordRead(const ItemPointer &location) {
  read_tuples[location.block].push_back(location.offset);
}

void Transaction::RecordWrite(const ItemPointer &location) {
  write_tuples[location.block].push_back(location.offset);
}

const std::map<oid_t, std::vector<oid_t>> &Transaction::GetReadTuples() {
  return read_tuples;
}

const std::map<oid_t, std::vector<oid_t>> &Transaction::GetWriteTuples() {
  return write_tuples;
}

void Transaction::ResetState(void) {
  read_tuples.clear();
  write_tuples.clear();
}

const std::string Transaction::GetInfo() const{
  std::ostringstream os;

  os << "\tTxn :: @" << this << " ID : " << std::setw(4) << txn_id
     << " Start Commit ID : " << std::setw(4) << start_cid
     << " End Commit ID : " << std::setw(4) << end_cid
     << " Result : " << result_;

  if (next == nullptr) {
    os << " Next : " << std::setw(4) << next;
  } else {
    os << " Next : " << std::setw(4) << next->txn_id;
  }

  os << " Ref count : " << std::setw(4) << ref_count << "\n";
  return os.str();
}

}  // End concurrency namespace
}  // End peloton namespace
