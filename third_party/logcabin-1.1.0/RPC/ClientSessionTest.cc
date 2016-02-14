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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "Core/CompatAtomic.h"
#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Event/Timer.h"
#include "Protocol/Common.h"
#include "RPC/ClientSession.h"

namespace LogCabin {
namespace RPC {
namespace {

typedef RPC::ClientSession::TimePoint TimePoint;

class RPCClientSessionTest : public ::testing::Test {
    RPCClientSessionTest()
        : eventLoop()
        , eventLoopThread(&Event::Loop::runForever, &eventLoop)
        , session()
        , remote(-1)
    {
        ClientSession::connectFn = ::connect;
        Address address("127.0.0.1", 0);
        address.refresh(Address::TimePoint::max());
        session = ClientSession::makeSession(eventLoop, address, 1024,
                                             TimePoint::max(),
                                             Core::Config());
        int socketPair[2];
        EXPECT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, socketPair));
        remote = socketPair[1];
        session->errorMessage.clear();
        session->messageSocket.reset(
            new MessageSocket(
                session->messageSocketHandler,
                eventLoop, socketPair[0], 1024));
    }

    ~RPCClientSessionTest()
    {
        ClientSession::connectFn = ::connect;
        session.reset();
        eventLoop.exit();
        eventLoopThread.join();
        EXPECT_EQ(0, close(remote));
    }

    Event::Loop eventLoop;
    std::thread eventLoopThread;
    std::shared_ptr<ClientSession> session;
    int remote;
};

std::string
str(const Core::Buffer& buffer)
{
    return std::string(static_cast<const char*>(buffer.getData()),
                       buffer.getLength());
}

Core::Buffer
buf(const char* stringLiteral)
{
    return Core::Buffer(const_cast<char*>(stringLiteral),
                        static_cast<uint32_t>(strlen(stringLiteral)),
                        NULL);
}

TEST_F(RPCClientSessionTest, handleReceivedMessage) {
    session->numActiveRPCs = 1;

    // Unexpected
    session->messageSocket->handler.handleReceivedMessage(1, buf("a"));

    // Normal
    session->timer.schedule(1000000000);
    session->responses[1] = new ClientSession::Response();
    session->messageSocket->handler.handleReceivedMessage(1, buf("b"));
    EXPECT_EQ(ClientSession::Response::HAS_REPLY,
              session->responses[1]->status);
    EXPECT_EQ("b", str(session->responses[1]->reply));
    EXPECT_EQ(0U, session->numActiveRPCs);
    EXPECT_FALSE(session->timer.isScheduled());

    // Already ready
    LogCabin::Core::Debug::setLogPolicy({{"", "ERROR"}});
    session->messageSocket->handler.handleReceivedMessage(1, buf("c"));
    EXPECT_EQ("b", str(session->responses[1]->reply));
    EXPECT_EQ(0U, session->numActiveRPCs);
}

TEST_F(RPCClientSessionTest, handleReceivedMessage_ping) {
    // spurious
    session->messageSocket->handler.handleReceivedMessage(
        Protocol::Common::PING_MESSAGE_ID, Core::Buffer());

    // ping requested
    session->numActiveRPCs = 1;
    session->activePing = true;
    session->messageSocket->handler.handleReceivedMessage(
        Protocol::Common::PING_MESSAGE_ID, Core::Buffer());
    session->numActiveRPCs = 0;
    EXPECT_FALSE(session->activePing);
    EXPECT_TRUE(session->timer.isScheduled());
}

TEST_F(RPCClientSessionTest, handleDisconnect) {
    session->messageSocket->handler.handleDisconnect();
    EXPECT_EQ("Disconnected from server 127.0.0.1 (resolved to 127.0.0.1:0)",
              session->errorMessage);
}

TEST_F(RPCClientSessionTest, handleTimerEvent) {
    // spurious
    std::unique_ptr<MessageSocket> oldMessageSocket;
    std::swap(oldMessageSocket, session->messageSocket);
    session->timer.handleTimerEvent();
    std::swap(oldMessageSocket, session->messageSocket);
    session->timer.handleTimerEvent();
    // make sure no actions were taken:
    EXPECT_FALSE(session->timer.isScheduled());
    EXPECT_EQ("", session->errorMessage);

    // need to send ping
    session->numActiveRPCs = 1;
    session->timer.handleTimerEvent();
    EXPECT_TRUE(session->activePing);
    char b;
    EXPECT_EQ(1U, read(remote, &b, 1));
    EXPECT_TRUE(session->timer.isScheduled());

    // need to time out session
    session->numActiveRPCs = 1;
    session->timer.handleTimerEvent();
    EXPECT_EQ("Server 127.0.0.1 (resolved to 127.0.0.1:0) timed out",
              session->errorMessage);
    session->numActiveRPCs = 0;
}

TEST_F(RPCClientSessionTest, constructor) {
    Address address("127.0.0.1", 0);
    address.refresh(Address::TimePoint::max());
    auto session2 = ClientSession::makeSession(eventLoop,
                                               address,
                                               1024,
                                               TimePoint::max(),
                                               Core::Config());
    EXPECT_EQ("127.0.0.1 (resolved to 127.0.0.1:0)",
              session2->address.toString());
    EXPECT_EQ("Failed to connect socket to 127.0.0.1 "
              "(resolved to 127.0.0.1:0): Connection refused",
              session2->errorMessage);
    EXPECT_EQ("Closed session: Failed to connect socket to 127.0.0.1 "
              "(resolved to 127.0.0.1:0): Connection refused",
              session2->toString());
    EXPECT_FALSE(session2->messageSocket);

    auto session3 = ClientSession::makeSession(eventLoop,
                                               Address("i n v a l i d", 0),
                                               1024,
                                               TimePoint::max(),
                                               Core::Config());
    EXPECT_EQ("Failed to resolve i n v a l i d (resolved to Unspecified)",
              session3->errorMessage);
    EXPECT_EQ("Closed session: Failed to resolve i n v a l i d "
              "(resolved to Unspecified)",
              session3->toString());
    EXPECT_FALSE(session3->messageSocket);
}

struct ConnectInProgress
{
    ConnectInProgress()
        : pipeFds()
    {
        int r = pipe(pipeFds);
        if (r != 0)
            PANIC("failed to create pipe: %s", strerror(errno));
    }
    ~ConnectInProgress() {
        if (pipeFds[0] >= 0)
            close(pipeFds[0]);
        if (pipeFds[1] >= 0)
            close(pipeFds[1]);
    }
    int operator()(int sockfd,
                    const struct sockaddr *addr,
                    socklen_t addrlen) {
        // Unfortunately, the unconnected socket generates epoll events if left
        // alone. Replace it with a pipe. Use the read end of the pipe so that
        // it's never writable
        int r = dup2(pipeFds[0], sockfd);
        EXPECT_LE(0, r);
        errno = EINPROGRESS;
        return -1;
    }
    int pipeFds[2]; // = {read, write}
};

TEST_F(RPCClientSessionTest, constructor_timeout_TimingSensitive) {
    ConnectInProgress c;
    ClientSession::connectFn = std::ref(c);
    Address address("127.0.0.1", 0);
    address.refresh(Address::TimePoint::max());
    auto start = Core::Time::SystemClock::now();
    auto session2 = ClientSession::makeSession(
        eventLoop,
        address,
        1024,
        Core::Time::SteadyClock::now() + std::chrono::milliseconds(5),
        Core::Config());
    auto end = Core::Time::SystemClock::now();
    uint64_t elapsedMs =
        uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(
                            end - start).count());
    EXPECT_LE(5U, elapsedMs);
    EXPECT_GE(100U, elapsedMs);
    ClientSession::connectFn = ::connect;
}

TEST_F(RPCClientSessionTest, makeSession) {
    EXPECT_EQ(session.get(), session->self.lock().get());
}

TEST_F(RPCClientSessionTest, makeErrorSession) {
    std::shared_ptr<ClientSession> esession =
        ClientSession::makeErrorSession(eventLoop, "my error msg");
    EXPECT_EQ("my error msg", esession->getErrorMessage());
    OpaqueClientRPC rpc = esession->sendRequest(buf("hi"));
    rpc.update();
    EXPECT_EQ(OpaqueClientRPC::Status::ERROR, rpc.getStatus());
    EXPECT_FALSE(rpc.session);
    EXPECT_EQ("", str(rpc.reply));
    EXPECT_EQ("my error msg", rpc.errorMessage);
    EXPECT_EQ(0U, esession->responses.size());
}

TEST_F(RPCClientSessionTest, destructor) {
    // nothing visible to test
}

TEST_F(RPCClientSessionTest, sendRequest) {
    EXPECT_EQ(0U, session->nextMessageId);
    session->activePing = true;
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    EXPECT_EQ(1U, session->numActiveRPCs);
    EXPECT_FALSE(session->activePing);
    EXPECT_TRUE(session->timer.isScheduled());
    EXPECT_EQ(session, rpc.session);
    EXPECT_EQ(0U, rpc.responseToken);
    EXPECT_EQ(OpaqueClientRPC::Status::NOT_READY, rpc.getStatus());
    EXPECT_EQ(1U, session->nextMessageId);
    auto it = session->responses.find(0);
    ASSERT_TRUE(it != session->responses.end());
    ClientSession::Response& response = *it->second;
    EXPECT_EQ(ClientSession::Response::WAITING,
              response.status);
}

TEST_F(RPCClientSessionTest, getErrorMessage) {
    EXPECT_EQ("", session->getErrorMessage());
    session->errorMessage = "x";
    EXPECT_EQ("x", session->getErrorMessage());
}

TEST_F(RPCClientSessionTest, toString) {
    EXPECT_EQ("Active session to 127.0.0.1 (resolved to 127.0.0.1:0)",
              session->toString());
}

TEST_F(RPCClientSessionTest, cancel) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    EXPECT_EQ(1U, session->numActiveRPCs);
    rpc.cancel();
    rpc.cancel(); // intentionally duplicated
    EXPECT_EQ(0U, session->numActiveRPCs);
    EXPECT_EQ(OpaqueClientRPC::Status::CANCELED, rpc.getStatus());
    EXPECT_FALSE(rpc.session);
    EXPECT_EQ(0U, rpc.reply.getLength());
    EXPECT_EQ("RPC canceled by user", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());

    // Cancel while there's a waiter is tested below in
    // waitCanceledWhileWaiting.
}

TEST_F(RPCClientSessionTest, updateCanceled) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    rpc.cancel();
    rpc.update();
    EXPECT_EQ(OpaqueClientRPC::Status::CANCELED, rpc.getStatus());
    EXPECT_EQ(0U, rpc.reply.getLength());
    EXPECT_EQ("RPC canceled by user", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());
}

TEST_F(RPCClientSessionTest, updateNotReady) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    rpc.update();
    EXPECT_EQ(OpaqueClientRPC::Status::NOT_READY, rpc.getStatus());
    EXPECT_EQ(0U, rpc.reply.getLength());
    EXPECT_EQ("", rpc.errorMessage);
    EXPECT_EQ(1U, session->responses.size());
}

TEST_F(RPCClientSessionTest, updateReady) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    auto it = session->responses.find(0);
    ASSERT_TRUE(it != session->responses.end());
    ClientSession::Response& response = *it->second;
    response.status = ClientSession::Response::HAS_REPLY;
    response.reply = buf("bye");
    rpc.update();
    EXPECT_EQ(OpaqueClientRPC::Status::OK, rpc.getStatus());
    EXPECT_FALSE(rpc.session);
    EXPECT_EQ("bye", str(rpc.reply));
    EXPECT_EQ("", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());
}

TEST_F(RPCClientSessionTest, updateError) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    session->errorMessage = "some error";
    rpc.update();
    EXPECT_EQ(OpaqueClientRPC::Status::ERROR, rpc.getStatus());
    EXPECT_FALSE(rpc.session);
    EXPECT_EQ("", str(rpc.reply));
    EXPECT_EQ("some error", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());
}

TEST_F(RPCClientSessionTest, waitNotReady) {
    // It's hard to test this one since it'll block.
    // TODO(ongaro): Use Core/ConditionVariable
}

TEST_F(RPCClientSessionTest, waitCanceled) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    rpc.cancel();
    rpc.waitForReply(TimePoint::max());
    EXPECT_EQ(OpaqueClientRPC::Status::CANCELED, rpc.getStatus());
}

TEST_F(RPCClientSessionTest, waitCanceledWhileWaiting) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    {
        ClientSession::Response& response = *session->responses.at(0);
        response.ready.callback = std::bind(&OpaqueClientRPC::cancel, &rpc);
        rpc.waitForReply(TimePoint::max());
    }
    EXPECT_EQ(OpaqueClientRPC::Status::CANCELED, rpc.getStatus());
    EXPECT_EQ("RPC canceled by user", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());
}

TEST_F(RPCClientSessionTest, waitReady) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    auto it = session->responses.find(0);
    ASSERT_TRUE(it != session->responses.end());
    ClientSession::Response& response = *it->second;
    response.status = ClientSession::Response::HAS_REPLY;
    response.reply = buf("bye");
    rpc.waitForReply(TimePoint::max());
    EXPECT_EQ(OpaqueClientRPC::Status::OK, rpc.getStatus());
    EXPECT_FALSE(rpc.session);
    EXPECT_EQ("bye", str(rpc.reply));
    EXPECT_EQ("", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());
}

TEST_F(RPCClientSessionTest, waitError) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    session->errorMessage = "some error";
    rpc.waitForReply(TimePoint::max());
    EXPECT_EQ(OpaqueClientRPC::Status::ERROR, rpc.getStatus());
    EXPECT_FALSE(rpc.session);
    EXPECT_EQ("", str(rpc.reply));
    EXPECT_EQ("some error", rpc.errorMessage);
    EXPECT_EQ(0U, session->responses.size());
}

TEST_F(RPCClientSessionTest, waitTimeout_now) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    rpc.waitForReply(RPC::ClientSession::Clock::now());
    EXPECT_EQ(OpaqueClientRPC::Status::NOT_READY, rpc.getStatus());
}

TEST_F(RPCClientSessionTest, waitTimeout_future) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    rpc.waitForReply(RPC::ClientSession::Clock::now() +
                     std::chrono::milliseconds(2));
    EXPECT_EQ(OpaqueClientRPC::Status::NOT_READY, rpc.getStatus());
}

TEST_F(RPCClientSessionTest, waitTimeout_futureThenOk) {
    OpaqueClientRPC rpc = session->sendRequest(buf("hi"));
    rpc.waitForReply(RPC::ClientSession::Clock::now() +
                     std::chrono::milliseconds(2));
    EXPECT_EQ(OpaqueClientRPC::Status::NOT_READY, rpc.getStatus());

    auto it = session->responses.find(0);
    ASSERT_TRUE(it != session->responses.end());
    ClientSession::Response& response = *it->second;
    response.status = ClientSession::Response::HAS_REPLY;
    response.reply = buf("bye");
    rpc.waitForReply(RPC::ClientSession::Clock::now() +
                     std::chrono::seconds(10));
    EXPECT_EQ(OpaqueClientRPC::Status::OK, rpc.getStatus());
}


} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
