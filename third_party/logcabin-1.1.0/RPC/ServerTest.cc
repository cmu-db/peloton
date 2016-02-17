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
#include <thread>

#include "build/Core/ProtoBufTest.pb.h"
#include "Core/Buffer.h"
#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Protocol/Common.h"
#include "RPC/ClientRPC.h"
#include "RPC/ClientSession.h"
#include "RPC/Server.h"
#include "RPC/ServerRPC.h"
#include "RPC/ServiceMock.h"

namespace LogCabin {
namespace RPC {
namespace {

using LogCabin::Protocol::Common::DEFAULT_PORT;
using LogCabin::Protocol::Common::MAX_MESSAGE_LENGTH;
typedef ClientSession::TimePoint TimePoint;

class RPCServerTest : public ::testing::Test {
    RPCServerTest()
        : eventLoop()
        , eventLoopThread(&Event::Loop::runForever, &eventLoop)
        , address("127.0.0.1", DEFAULT_PORT)
        , server(eventLoop, MAX_MESSAGE_LENGTH)
        , session()
        , service1(std::make_shared<ServiceMock>())
        , service2(std::make_shared<ServiceMock>())
        , service3(std::make_shared<ServiceMock>())
        , request()
        , reply()
    {
        address.refresh(Address::TimePoint::max());
        EXPECT_EQ("", server.bind(address));
        session = ClientSession::makeSession(
                        eventLoop,
                        address,
                        MAX_MESSAGE_LENGTH,
                        RPC::ClientSession::TimePoint::max(),
                        Core::Config());
        request.set_field_a(3);
        request.set_field_b(4);
        reply.set_field_a(5);
        reply.set_field_b(6);
    }
    void deinit() {
        eventLoop.exit();
        if (eventLoopThread.joinable())
            eventLoopThread.join();
    }
    void childDeathInit() {
        assert(!eventLoopThread.joinable());
        eventLoopThread = std::thread(&Event::Loop::runForever, &eventLoop);
    }
    ~RPCServerTest() {
        deinit();
    }
    Event::Loop eventLoop;
    std::thread eventLoopThread;
    Address address;
    Server server;
    std::shared_ptr<ClientSession> session;
    std::shared_ptr<ServiceMock> service1;
    std::shared_ptr<ServiceMock> service2;
    std::shared_ptr<ServiceMock> service3;
    ProtoBuf::TestMessage request;
    ProtoBuf::TestMessage reply;
};

TEST_F(RPCServerTest, handleRPC_normal) {
    server.registerService(1, service1, 1);
    service1->reply(0, request, reply);
    ClientRPC rpc(session, 1, 1, 0, request);
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
}

TEST_F(RPCServerTest, handleRPC_badHeader) {
    server.registerService(1, service1, 1);
    ClientRPC rpc;
    rpc.opaqueRPC = session->sendRequest(Core::Buffer());
    EXPECT_EQ(ClientRPC::Status::INVALID_REQUEST,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
}

TEST_F(RPCServerTest, handleRPC_badService) {
    ClientRPC rpc(session, 1, 1, 0, request);
    EXPECT_EQ(ClientRPC::Status::INVALID_SERVICE,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
}

// constructor: nothing to test

// destructor: nothing to test

// bind: nothing to test

TEST_F(RPCServerTest, registerService) {
    server.registerService(1, service1, 1);
    server.registerService(2, service2, 1);
    server.registerService(1, service2, 1);
    EXPECT_EQ(2U, server.services.size());
    service2->reply(0, request, reply);
    service2->reply(0, request, reply);
    ClientRPC rpc(session, 1, 1, 0, request);
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
    rpc = ClientRPC(session, 2, 1, 0, request);
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
}

} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
