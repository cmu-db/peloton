/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <cinttypes>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "Core/ConditionVariable.h"
#include "RPC/ServerRPC.h"
#include "RPC/Service.h"

#ifndef LOGCABIN_RPC_THREADDISPATCHSERVICE_H
#define LOGCABIN_RPC_THREADDISPATCHSERVICE_H

namespace LogCabin {
namespace RPC {

/**
 * This class is an adaptor to enable multi-threaded services.
 * This Service is intended to plug into a Server and run directly on the
 * Event::Loop thread. You provide it with another Service on the constructor,
 * and the job of this class is to manage a thread pool on which to call
 * your Service's handleRPC() method.
 */
class ThreadDispatchService : public Service {
  public:
    /**
     * Constructor.
     * \param threadSafeService
     *      The underlying service that will handle RPCs inside of worker
     *      threads spawned by this class.
     * \param minThreads
     *      The number of threads with which to start the thread pool.
     *      These will be created in the constructor.
     * \param maxThreads
     *      The maximum number of threads this class is allowed to use for its
     *      thread pool. The thread pool dynamically grows as needed up until
     *      this limit. This should be set to at least 'minThreads' and more
     *      than 0.
     */
    ThreadDispatchService(std::shared_ptr<Service> threadSafeService,
                          uint32_t minThreads, uint32_t maxThreads);

    /**
     * Destructor. This will attempt to join all threads and will close
     * sessions on RPCs that have not been serviced.
     */
    ~ThreadDispatchService();

    void handleRPC(ServerRPC serverRPC);
    std::string getName() const;

  private:
    /**
     * The main loop executed in workers.
     */
    void workerMain();

    /**
     * The service that will handle RPCs inside of worker thread spawned by
     * this class.
     */
    std::shared_ptr<Service> threadSafeService;

    /**
     * The maximum number of threads this class is allowed to use for its
     * thread pool.
     */
    const uint32_t maxThreads;

    /**
     * This mutex protects all of the members of this class defined below this
     * point.
     */
    std::mutex mutex;

    /**
     * The thread pool of workers that process RPCs.
     */
    std::vector<std::thread> threads;

    /**
     * The number of workers that are waiting for work (on the condition
     * variable). This is used to dynamically launch new workers when
     * necessary.
     */
    uint32_t numFreeWorkers;

    /**
     * Notifies workers that there are available RPCs to process or #exit has
     * been set. To wait on this, one needs to hold #mutex.
     */
    Core::ConditionVariable conditionVariable;

    /**
     * A flag to tell workers that they should exit.
     */
    bool exit;

    /**
     * The queue of work that worker threads pull from.
     */
    std::queue<ServerRPC> rpcQueue;

    // ThreadDispatchService is non-copyable.
    ThreadDispatchService(const ThreadDispatchService&) = delete;
    ThreadDispatchService& operator=(const ThreadDispatchService&) = delete;
}; // class ThreadDispatchService

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_THREADDISPATCHSERVICE_H */
