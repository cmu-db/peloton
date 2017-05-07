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
        delete [] local_hash_tables_;
    }

    void SetBarrier(boost::barrier *bar)
    {
        bar_ = bar;
    }

    void SetWorkerCount(uint64_t n_workers)
    {
        n_workers_ = n_workers;
    }

    void AddLocalHashTable(int64_t thread_id, utils::OAHashTable *hash_table)
    {
        LOG_DEBUG("Adding: vector size: %ld, thread id: %ld", n_local_hts_, long(thread_id));
        local_hash_tables_[int(thread_id)] = hash_table;
        LOG_DEBUG("Done: vector size: %ld, thread id: %ld", n_local_hts_, long(thread_id));
    }

    utils::OAHashTable* GetLocalHashTable(int32_t idx)
    {
        return local_hash_tables_[idx];
    }

    void AllocateVector(uint64_t n_local_hts)
    {
        n_local_hts_ = n_local_hts;
        local_hash_tables_ = new utils::OAHashTable*[n_local_hts_];
        LOG_DEBUG("vector size: %ld", n_local_hts_);
    }

private:
    Barrier(): bar_(nullptr) {}
    boost::barrier *bar_;
    std::atomic<uint64_t> n_workers_;
    uint64_t n_local_hts_;
    utils::OAHashTable **local_hash_tables_;
};

}
}
