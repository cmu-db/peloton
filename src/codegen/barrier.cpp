//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// barrier.cpp
//
// Identification: src/codegen/barrier.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>

#include "common/logger.h"
#include "codegen/barrier.h"
#include "codegen/utils/oa_hash_table.h"

namespace peloton {
namespace codegen {

void Barrier::InitInstance(Barrier *ins, uint64_t n_workers) {
  assert(n_workers > 0);
  ins->SetBarrier(new boost::barrier(n_workers));
  ins->SetWorkerCount(n_workers);
  ins->InitGlobalHashTableMergeLock();
}

void Barrier::MasterWait() {
  while (n_workers_ > 0)
    ;
}

void Barrier::Destroy() {
  if (bar_ != nullptr) {
    delete bar_;
  }
}

}
}
