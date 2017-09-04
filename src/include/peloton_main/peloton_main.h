#pragma once

#include "common/thread_pool.h"
#include "network/network_manager.h"
#include "concurrency/epoch_manager.h"
#include "gc/gc_manager.h"

namespace peloton {

    //===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

    class PelotonMain {
    private:
        ThreadPool thread_pool;
        network::NetworkManager network_manager;
        concurrency::EpochManager *epoch_manager;
        gc::GCManager *gc_manager;
    public:
        PelotonMain();

        void Initialize();

        void Shutdown();

        static void SetUpThread();

        static void TearDownThread();

        static PelotonMain &GetInstance();

        ThreadPool &GetThreadPool();

        network::NetworkManager &GetNetworkManager();

        concurrency::EpochManager *GetEpochManager();

        gc::GCManager *GetGCManager();
    };

}