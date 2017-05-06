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

#include <boost/thread/barrier.hpp>
#include <vector>
#include <atomic>

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

    void AddLocalHashTable(UNUSED_ATTRIBUTE int64_t thread_id, UNUSED_ATTRIBUTE utils::OAHashTable *hash_table)
    {
        LOG_DEBUG("vector size: %zd, thread id: %ld", local_hash_tables_.size(), long(thread_id));
        local_hash_tables_[int(thread_id)] = hash_table;
    }

    void AllocateVector(uint64_t n_workers)
    {
        local_hash_tables_.resize(n_workers);
        LOG_DEBUG("vector size: %zd", local_hash_tables_.size());
    }

private:
    Barrier(): bar_(nullptr) {}
    boost::barrier *bar_;
    std::atomic<uint64_t> n_workers_;
    std::vector<utils::OAHashTable*> local_hash_tables_;
};

}
}
