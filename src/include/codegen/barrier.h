#pragma once

#include <boost/thread/barrier.hpp>
#include <atomic>

namespace peloton {
namespace codegen {

class Barrier
{

public:
    static void InitInstance(Barrier *ins, uint64_t n_workers);

    void BarrierWait();

    void WorkerFinish();

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

private:
    Barrier(): bar_(nullptr) {}
    boost::barrier *bar_;
    std::atomic<uint64_t> n_workers_;
};

}
}
