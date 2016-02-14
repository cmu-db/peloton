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

#include <gtest/gtest.h>
#include <unistd.h>

#include "Core/CompatAtomic.h"
#include "RPC/ThreadDispatchService.h"

namespace LogCabin {
namespace RPC {
namespace {

class EchoService : public RPC::Service {
    EchoService()
        : sleepMicros(0)
        , count(0)
    {
    }
    void handleRPC(RPC::ServerRPC serverRPC) {
        usleep(sleepMicros);
        ++count;
    }
    std::string getName() const {
        return "EchoService";
    }
    std::atomic<uint32_t> sleepMicros;
    std::atomic<uint32_t> count;
};


class RPCThreadDispatchServiceTest : public ::testing::Test {
    RPCThreadDispatchServiceTest()
        : echoService(std::make_shared<EchoService>())
    {
    }
    std::shared_ptr<EchoService> echoService;
};

TEST_F(RPCThreadDispatchServiceTest, constructor)
{
    ThreadDispatchService dispatchService(echoService,
                                          5, 6);
    // Give the threads a chance to start up
    for (uint32_t i = 0; i < 10; ++i) {
        {
            std::lock_guard<std::mutex> lockGuard(dispatchService.mutex);
            if (dispatchService.numFreeWorkers == 5)
                break;
        }
        usleep(1000);
    }
    std::lock_guard<std::mutex> lockGuard(dispatchService.mutex);
    EXPECT_EQ(5U, dispatchService.threads.size());
    EXPECT_EQ(5U, dispatchService.numFreeWorkers);
}

TEST_F(RPCThreadDispatchServiceTest, destructor)
{
    // The assertion in std::thread::~thread guarantees that the destructor has
    // joined all running threads. So this test really just needs to make sure
    // we close the session on all outstanding RPCs. Unfortunately, that'd
    // require a fair bit of plumbing to verify, and it's clear from the code
    // that it does this.
}

TEST_F(RPCThreadDispatchServiceTest, handleRPC)
{
    // should spawn threads up until 2
    ThreadDispatchService dispatchService(echoService,
                                          0, 2);
    echoService->sleepMicros = 1000;
    for (uint32_t i = 0; i < 10; ++i)
        dispatchService.handleRPC(ServerRPC());
    echoService->sleepMicros = 0;
    while (echoService->count < 10)
        usleep(1000);
    EXPECT_EQ(10U, echoService->count);
    EXPECT_EQ(2U, dispatchService.threads.size());
}

TEST_F(RPCThreadDispatchServiceTest, workerMain)
{
    // most of this is tested already in the other tests
}

} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
