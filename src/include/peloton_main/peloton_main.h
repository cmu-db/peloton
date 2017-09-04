#pragma once

#include "common/thread_pool.h"

namespace peloton {

    //===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

    class PelotonMain {
    private:
        peloton::ThreadPool thread_pool;
    public:
        void Initialize();

        void Shutdown();

        static void SetUpThread();

        static void TearDownThread();

        static PelotonMain &GetInstance();

        ThreadPool &GetThreadPool();
    };

}