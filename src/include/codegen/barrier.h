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
#include <mutex>

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

    void MergeToGlobalHashTable(utils::OAHashTable *local_ht)
    {
        // bool expected = true;
        // while (!can_merge_.compare_exchange_strong(expected, false))
        // {
        //     expected = true;
        // }
        // global_hash_table_->Merge(local_ht);
        // can_merge_ = true;
        // bool expected = true;
        // if (can_merge_.compare_exchange_strong(expected, false)) {
        //     global_hash_table_->Merge(local_ht);
        // } else {
        //     MergeToGlobalHashTable(local_ht);
        // }
        // std::lock_guard<std::mutex> lock{mx};
        // {
        //     std::lock(mx);
            // if (global_hash_table_ == nullptr) {
            //     global_hash_table_ =
            // }
            global_hash_table_->Merge(local_ht);
            LOG_DEBUG("local hash table size: %ld", local_ht->NumEntries());
            LOG_DEBUG("global hash table size: %ld", global_hash_table_->NumEntries());
        // }
    }

    void AllocateVector(uint64_t n_local_hts)
    {
        n_local_hts_ = n_local_hts;
        local_hash_tables_ = new utils::OAHashTable*[n_local_hts_];
        LOG_DEBUG("vector size: %ld", n_local_hts_);
    }

    void InitGlobalHashTable(uint64_t key_size, uint64_t value_size)
    {
        PL_MEMSET(raw_hash_table, 1, sizeof(raw_hash_table));
        global_hash_table_ = reinterpret_cast<codegen::utils::OAHashTable *>(raw_hash_table);
        global_hash_table_->Init(key_size, value_size);
    }

    utils::OAHashTable* GetGlobalHashTable()
    {
        return global_hash_table_;
    }

private:
    Barrier(): bar_(nullptr) {}
    boost::barrier *bar_;
    std::atomic<uint64_t> n_workers_;
    uint64_t n_local_hts_;
    utils::OAHashTable **local_hash_tables_;

    int8_t raw_hash_table[sizeof(utils::OAHashTable)];
    utils::OAHashTable *global_hash_table_;

    std::atomic<bool> can_merge_{true};
    std::mutex mx;
};

}
}
