/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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

#include "build/Protocol/Client.pb.h"
#include "include/LogCabin/Debug.h"
#include "Client/SessionManager.h"
#include "Core/Config.h"
#include "Event/Loop.h"
#include "Protocol/Common.h"
#include "RPC/ClientSession.h"
#include "RPC/Server.h"
#include "RPC/ServiceMock.h"

namespace LogCabin {
namespace Client {
namespace {

typedef RPC::Address::TimePoint TimePoint;

class ClientSessionManagerTest : public ::testing::Test {
    ClientSessionManagerTest()
        : eventLoop()
        , eventLoopThread(&Event::Loop::runForever, &eventLoop)
        , config()
        , clusterUUID()
        , serverId()
        , sessionManager(eventLoop, config)
        , address("127.0.0.1", Protocol::Common::DEFAULT_PORT)
        , service()
        , server()

    {
        address.refresh(RPC::Address::TimePoint::max());
        service = std::make_shared<RPC::ServiceMock>();
        server.reset(new RPC::Server(eventLoop,
                                     Protocol::Common::MAX_MESSAGE_LENGTH));
        EXPECT_EQ("", server->bind(address));
        server->registerService(Protocol::Common::ServiceId::CLIENT_SERVICE,
                                service, 1);
    }

    ~ClientSessionManagerTest() {
        eventLoop.exit();
        eventLoopThread.join();
    }

    Event::Loop eventLoop;
    std::thread eventLoopThread;
    Core::Config config;
    SessionManager::ClusterUUID clusterUUID;
    SessionManager::ServerId serverId;
    SessionManager sessionManager;
    RPC::Address address;
    std::shared_ptr<RPC::ServiceMock> service;
    std::unique_ptr<RPC::Server> server;
};

TEST_F(ClientSessionManagerTest, createSession_makeSessionFailed)
{
    // ClientSession::makeSession returns invalid session
    RPC::Address nullAddress;
    auto session = sessionManager.createSession(
        nullAddress,
        TimePoint::max());
    EXPECT_EQ("Failed to resolve No address given",
              session->getErrorMessage());
}

TEST_F(ClientSessionManagerTest, createSession_verifyRPCFailed)
{
    // makeSession succeeded but RPC failed
    Protocol::Client::VerifyRecipient::Request request;
    service->closeSession(Protocol::Client::OpCode::VERIFY_RECIPIENT,
                          request);
    auto session = sessionManager.createSession(
        address,
        TimePoint::max());
    EXPECT_EQ("Verifying recipient with 127.0.0.1 "
              "(resolved to 127.0.0.1:5254) failed "
              "(after connecting over TCP)",
              session->getErrorMessage());
}

TEST_F(ClientSessionManagerTest, createSession_verifyGood)
{
    Protocol::Client::VerifyRecipient::Request request;
    request.set_server_id(3);
    Protocol::Client::VerifyRecipient::Response response;
    response.set_cluster_uuid("foo");
    response.set_ok(true);
    service->reply(Protocol::Client::OpCode::VERIFY_RECIPIENT,
                   request, response);
    serverId.set(3);
    auto session = sessionManager.createSession(
        address,
        TimePoint::max(),
        &clusterUUID,
        &serverId);
    EXPECT_EQ("", session->getErrorMessage());
    EXPECT_EQ("foo", clusterUUID.getOrDefault());
}

TEST_F(ClientSessionManagerTest, createSession_verifyBad)
{
    Protocol::Client::VerifyRecipient::Request request;
    request.set_cluster_uuid("foo");
    request.set_server_id(3);
    Protocol::Client::VerifyRecipient::Response response;
    response.set_cluster_uuid("bar");
    response.set_ok(false);
    service->reply(Protocol::Client::OpCode::VERIFY_RECIPIENT,
                   request, response);
    clusterUUID.set("foo");
    serverId.set(3);
    Core::Debug::setLogPolicy({ // expect error
        {"Client/SessionManager.cc", "SILENT"}
    });
    auto session = sessionManager.createSession(
        address,
        TimePoint::max(),
        &clusterUUID,
        &serverId);
    EXPECT_EQ("Verifying recipient with 127.0.0.1 "
              "(resolved to 127.0.0.1:5254) failed "
              "(after connecting over TCP)",
              session->getErrorMessage());
    EXPECT_EQ("foo", clusterUUID.getOrDefault());
}

} // namespace LogCabin::Client::<anonymous>
} // namespace LogCabin::Client
} // namespace LogCabin
