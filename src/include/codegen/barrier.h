//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// barrier.h
//
// Identification: src/include/codegen/barrier.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <boost/thread/barrier.hpp>

#include "common/logger.h"
#include "codegen/codegen.h"
#include "codegen/utils/oa_hash_table.h"

namespace peloton {
namespace codegen {

class Barrier
{

public:
    static void InitInstance(Barrier *ins, uint64_t n_workers);

    bool BarrierWait()
    {
        return bar_->wait();
    }

    void WorkerFinish()
    {
        --n_workers_;
    }

    void MasterWait();

    ~Barrier()
    {
        delete bar_;
    }

    void SetBarrier(boost::barrier *bar)
    {
        bar_ = bar;
    }

    void SetWorkerCount(uint64_t n_workers)
    {
        n_workers_ = n_workers;
    }

    void MergeToGlobalHashTable(utils::OAHashTable *global_ht, utils::OAHashTable *local_ht);

private:

    void InitGlobalHashTableMergeLock() {
      global_hash_table_merge_lock_.store(false);
    }

    Barrier(): bar_(nullptr) {}
    boost::barrier *bar_;
    std::atomic<uint64_t> n_workers_;

    std::atomic<bool> global_hash_table_merge_lock_;
};

}
}
