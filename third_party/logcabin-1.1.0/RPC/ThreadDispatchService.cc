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

#include <assert.h>

#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "RPC/ThreadDispatchService.h"

namespace LogCabin {
namespace RPC {

ThreadDispatchService::ThreadDispatchService(
        std::shared_ptr<Service> threadSafeService,
        uint32_t minThreads,
        uint32_t maxThreads)
    : threadSafeService(threadSafeService)
    , maxThreads(maxThreads)
    , mutex()
    , threads()
    , numFreeWorkers(0)
    , conditionVariable()
    , exit(false)
    , rpcQueue()
{
    assert(minThreads <= maxThreads);
    assert(0 < maxThreads);
    for (uint32_t i = 0; i < minThreads; ++i)
        threads.emplace_back(&ThreadDispatchService::workerMain, this);
}

ThreadDispatchService::~ThreadDispatchService()
{
    // Signal the threads to exit.
    {
        std::lock_guard<std::mutex> lockGuard(mutex);
        exit = true;
        conditionVariable.notify_all();
    }

    // Join the threads.
    while (!threads.empty()) {
        threads.back().join();
        threads.pop_back();
    }

    // Close the sessions of any remaining RPCs that didn't get processed.
    while (!rpcQueue.empty()) {
        rpcQueue.front().closeSession();
        rpcQueue.pop();
    }
}

void
ThreadDispatchService::handleRPC(ServerRPC serverRPC)
{
    std::lock_guard<std::mutex> lockGuard(mutex);
    assert(!exit);
    rpcQueue.push(std::move(serverRPC));
    if (numFreeWorkers == 0 && threads.size() < maxThreads)
        threads.emplace_back(&ThreadDispatchService::workerMain, this);
    conditionVariable.notify_one();
}

std::string
ThreadDispatchService::getName() const
{
    return threadSafeService->getName();
}

void
ThreadDispatchService::workerMain()
{
    Core::ThreadId::setName(
        Core::StringUtil::format("%s(%lu)",
                                 threadSafeService->getName().c_str(),
                                 Core::ThreadId::getId()));
    while (true) {
        ServerRPC rpc;
        { // find an RPC to process
            std::unique_lock<std::mutex> lockGuard(mutex);
            ++numFreeWorkers;
            while (!exit && rpcQueue.empty())
                conditionVariable.wait(lockGuard);
            --numFreeWorkers;
            if (exit)
                return;
            rpc = std::move(rpcQueue.front());
            rpcQueue.pop();
        }
        // execute RPC handler
        threadSafeService->handleRPC(std::move(rpc));
    }
}

} // namespace LogCabin::RPC
} // namespace LogCabin
