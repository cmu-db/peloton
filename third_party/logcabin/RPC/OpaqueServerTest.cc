/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Protocol/Common.h"
#include "RPC/Address.h"
#include "RPC/OpaqueServer.h"
#include "RPC/OpaqueServerRPC.h"

namespace LogCabin {
namespace RPC {
namespace {

class MyServerHandler : public OpaqueServer::Handler {
    MyServerHandler()
        : lastRPC()
    {
    }
    void handleRPC(OpaqueServerRPC serverRPC) {
        lastRPC.reset(new OpaqueServerRPC(std::move(serverRPC)));
    }
    std::unique_ptr<OpaqueServerRPC> lastRPC;
};

class RPCOpaqueServerTest : public ::testing::Test {
    RPCOpaqueServerTest()
        : loop()
        , address("127.0.0.1", Protocol::Common::DEFAULT_PORT)
        , rpcHandler()
        , server(rpcHandler, loop, 1024)
        , fd1(-1)
        , fd2(-1)
    {
        address.refresh(RPC::Address::TimePoint::max());
        EXPECT_EQ("", server.bind(address));
        int fds[2];
        EXPECT_EQ(0, pipe(fds));
        fd1 = fds[0];
        fd2 = fds[1];
    }
    ~RPCOpaqueServerTest() {
        if (fd1 != -1)
            EXPECT_EQ(0, close(fd1));
        if (fd2 != -1)
            EXPECT_EQ(0, close(fd2));
    }
    Event::Loop loop;
    Address address;
    MyServerHandler rpcHandler;
    OpaqueServer server;
    int fd1;
    int fd2;
};

TEST_F(RPCOpaqueServerTest, MessageSocketHandler_handleReceivedMessage) {
    auto socket = OpaqueServer::SocketWithHandler::make(&server, fd1);
    server.sockets.insert(socket);
    fd1 = -1;
    socket->handler.handleReceivedMessage(1, Core::Buffer(NULL, 3, NULL));
    ASSERT_TRUE(rpcHandler.lastRPC.get());
    EXPECT_EQ(3U, rpcHandler.lastRPC->request.getLength());
    EXPECT_EQ(0U, rpcHandler.lastRPC->response.getLength());
    EXPECT_EQ(socket, rpcHandler.lastRPC->socket.lock());
    EXPECT_EQ(1U, rpcHandler.lastRPC->messageId);
}

TEST_F(RPCOpaqueServerTest, MessageSocketHandler_handleReceivedMessage_ping) {
    auto socket = OpaqueServer::SocketWithHandler::make(&server, fd1);
    server.sockets.insert(socket);
    fd1 = -1;
    socket->handler.handleReceivedMessage(
        Protocol::Common::PING_MESSAGE_ID, Core::Buffer());
    ASSERT_FALSE(rpcHandler.lastRPC);
    EXPECT_EQ(1U, socket->monitor.outboundQueue.size());
    Core::Buffer& buf = socket->monitor.outboundQueue.at(0).message;
    EXPECT_EQ(0U, buf.getLength());
}

TEST_F(RPCOpaqueServerTest,
       MessageSocketHandler_handleReceivedMessage_version) {
    auto socket = OpaqueServer::SocketWithHandler::make(&server, fd1);
    server.sockets.insert(socket);
    fd1 = -1;
    socket->handler.handleReceivedMessage(
        Protocol::Common::VERSION_MESSAGE_ID, Core::Buffer());
    ASSERT_FALSE(rpcHandler.lastRPC);
    EXPECT_EQ(1U, socket->monitor.outboundQueue.size());
    Core::Buffer& buf = socket->monitor.outboundQueue.at(0).message;
    using Protocol::Common::VersionMessage::Response;
    EXPECT_EQ(sizeof(Response), buf.getLength());
    Response* response = static_cast<Response*>(buf.getData());
    EXPECT_EQ(1U, be16toh(response->maxVersionSupported));
}


TEST_F(RPCOpaqueServerTest, MessageSocketHandler_handleDisconnect) {
    auto socket = OpaqueServer::SocketWithHandler::make(&server, fd1);
    server.sockets.insert(socket);
    fd1 = -1;
    socket->handler.handleDisconnect();
    EXPECT_EQ(0U, server.sockets.size());
    EXPECT_TRUE(socket->handler.server == NULL);
    socket->monitor.close();
}

void
clientMain(int& fd, Address& address, OpaqueServer& server)
{
    fd = socket(AF_INET, SOCK_STREAM, 0);
    int r = connect(fd,
                    address.getSockAddr(),
                    address.getSockAddrLen());
    if (r < 0)
        PANIC("connect error: %s", strerror(errno));

    server.eventLoop.exit();
};

TEST_F(RPCOpaqueServerTest, BoundListener_handleFileEvent) {
    int clientFd = -1;
    std::thread clientThread(clientMain,
                             std::ref(clientFd),
                             std::ref(address),
                             std::ref(server));
    server.eventLoop.runForever();
    clientThread.join();
    EXPECT_EQ(1U, server.sockets.size());
    close(clientFd);
}

TEST_F(RPCOpaqueServerTest, bind_good) {
    Address address2("127.0.0.1", 5253);
    address2.refresh(Address::TimePoint::max());
    EXPECT_EQ("", server.bind(address2));
    Address address3("127.0.0.1", 5255);
    address3.refresh(Address::TimePoint::max());
    EXPECT_EQ("", server.bind(address3));
    EXPECT_EQ(3U, server.boundListeners.size());
}

TEST_F(RPCOpaqueServerTest, bind_badAddress) {
    Address address2("", 0);
    std::string error = server.bind(address2);
    EXPECT_TRUE(error.find("Can't listen on invalid address") != error.npos)
        << error;
}

TEST_F(RPCOpaqueServerTest, bind_portTaken) {
    Address address2("127.0.0.1", Protocol::Common::DEFAULT_PORT);
    address2.refresh(Address::TimePoint::max());
    std::string error = server.bind(address2);
    EXPECT_TRUE(error.find("in use") != error.npos)
        << error;
}

} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
