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

/**
 * \file
 * This is a simple end-to-end test of the basic RPC system.
 */

#include <string.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include <thread>

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Event/Timer.h"
#include "Protocol/Common.h"
#include "RPC/ClientSession.h"
#include "RPC/OpaqueClientRPC.h"
#include "RPC/OpaqueServer.h"
#include "RPC/OpaqueServerRPC.h"
#include "RPC/Service.h"
#include "RPC/ThreadDispatchService.h"

namespace LogCabin {
namespace {

typedef RPC::OpaqueClientRPC::TimePoint TimePoint;

class ReplyTimer : public Event::Timer {
    ReplyTimer(Event::Loop& eventLoop,
               RPC::OpaqueServerRPC serverRPC,
               uint32_t delayMicros)
        : Timer()
        , serverRPC(std::move(serverRPC))
        , monitor(eventLoop, *this)
    {
        schedule(delayMicros * 1000);
    }
    ~ReplyTimer() {
        monitor.disableForever();
    }
    void handleTimerEvent() {
        VERBOSE("Ok responding");
        serverRPC.sendReply();
        delete this;
    }
    RPC::OpaqueServerRPC serverRPC;
    // You aren't really supposed to put a monitor in an Event::Timer, but the
    // memory management here is weird. Just make sure to call
    // monitor.disableForever() as the first thing in the destructor.
    Event::Timer::Monitor monitor;
};

class EchoServer : public RPC::OpaqueServer::Handler {
    explicit EchoServer(Event::Loop& eventLoop)
        : eventLoop(eventLoop)
        , delayMicros(0)
    {
    }
    void handleRPC(RPC::OpaqueServerRPC serverRPC) {
        serverRPC.response = std::move(serverRPC.request);
        serverRPC.sendReply();
        VERBOSE("Delaying response for %u microseconds", delayMicros);
        new ReplyTimer(eventLoop, std::move(serverRPC), delayMicros);
    }
    Event::Loop& eventLoop;
    uint32_t delayMicros;
};

class RPCClientServerTest : public ::testing::Test {
    RPCClientServerTest()
        : config()
        , clientEventLoop()
        , serverEventLoop()
        , clientEventLoopThread(&Event::Loop::runForever, &clientEventLoop)
        , serverEventLoopThread(&Event::Loop::runForever, &serverEventLoop)
        , address("127.0.0.1", Protocol::Common::DEFAULT_PORT)
        , rpcHandler(serverEventLoop)
        , server(rpcHandler, serverEventLoop, 1024)
        , clientSession()
    {
        address.refresh(RPC::Address::TimePoint::max());
        EXPECT_EQ("", server.bind(address));
        config.set("tcpHeartbeatTimeoutMilliseconds", 1000);
        init();
    }

    void init()
    {
        clientSession = RPC::ClientSession::makeSession(
                            clientEventLoop, address, 1024,
                            RPC::ClientSession::TimePoint::max(),
                            config);
    }


    ~RPCClientServerTest()
    {
        serverEventLoop.exit();
        clientEventLoop.exit();
        serverEventLoopThread.join();
        clientEventLoopThread.join();
    }

    Core::Config config;
    Event::Loop clientEventLoop;
    Event::Loop serverEventLoop;
    std::thread clientEventLoopThread;
    std::thread serverEventLoopThread;
    RPC::Address address;
    EchoServer rpcHandler;
    RPC::OpaqueServer server;
    std::shared_ptr<RPC::ClientSession> clientSession;
};

// Make sure the server can echo back messages.
TEST_F(RPCClientServerTest, echo) {
    for (uint32_t bufLen = 0; bufLen < 1024; ++bufLen) {
        char buf[bufLen];
        for (uint32_t i = 0; i < bufLen; ++i)
            buf[i] = char(i);
        RPC::OpaqueClientRPC rpc = clientSession->sendRequest(
                                        Core::Buffer(buf, bufLen, NULL));
        rpc.waitForReply(TimePoint::max());
        EXPECT_EQ(RPC::OpaqueClientRPC::Status::OK, rpc.getStatus())
            << rpc.getErrorMessage();
        Core::Buffer& reply = *rpc.peekReply();
        EXPECT_EQ(bufLen, reply.getLength());
        EXPECT_EQ(0, memcmp(reply.getData(), buf, bufLen));
    }
}

// Test the RPC timeout (ping) mechanism.
TEST_F(RPCClientServerTest, timeout_TimingSensitive) {
    config.set("tcpHeartbeatTimeoutMilliseconds", 12);
    init();
    EXPECT_EQ(12UL * 1000 * 1000, clientSession->PING_TIMEOUT_NS);
    rpcHandler.delayMicros = 14 * 1000;

    // The server should not time out, since the serverEventLoopThread should
    // respond to pings.
    RPC::OpaqueClientRPC rpc = clientSession->sendRequest(Core::Buffer());
    rpc.waitForReply(TimePoint::max());
    EXPECT_EQ("", rpc.getErrorMessage());

    // This time, if we don't let the server event loop run, the RPC should
    // time out.
    Event::Loop::Lock blockPings(serverEventLoop);
    RPC::OpaqueClientRPC rpc2 = clientSession->sendRequest(Core::Buffer());
    rpc2.waitForReply(TimePoint::max());
    EXPECT_EQ("Server 127.0.0.1 (resolved to 127.0.0.1:5254) timed out",
              rpc2.getErrorMessage());

}

} // namespace LogCabin::<anonymous>
} // namespace LogCabin
