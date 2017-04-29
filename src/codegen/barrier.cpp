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

#include "codegen/barrier.h"

#include "common/logger.h"

namespace peloton {
namespace codegen {

void Barrier::InitInstance(Barrier *ins, uint64_t n_workers)
{
    ins->SetBarrier(new boost::barrier(n_workers));
    ins->SetWorkerCount(n_workers);
}

void Barrier::BarrierWait()
{
    bar_->wait();
}

void Barrier::WorkerFinish()
{
    --n_workers_;
}

void Barrier::MasterWait()
{
    while(n_workers_ > 0);
}

}
}
