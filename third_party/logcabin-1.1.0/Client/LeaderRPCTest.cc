/* Copyright (c) 2012 Stanford University
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

#include "Client/Backoff.h"
#include "Client/LeaderRPC.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Event/Loop.h"
#include "Protocol/Common.h"
#include "RPC/ClientSession.h"
#include "RPC/Server.h"
#include "RPC/ServiceMock.h"

namespace LogCabin {
namespace Client {
namespace {

using Protocol::Client::OpCode;
typedef LeaderRPCBase::Clock Clock;
typedef LeaderRPCBase::TimePoint TimePoint;

class ClientLeaderRPCTest : public ::testing::Test {
  public:
    ClientLeaderRPCTest()
        : eventLoop()
        , sessionCreationBackoff(1, 1)
        , service()
        , server()
        , eventLoopThread()
        , config()
        , clusterUUID()
        , sessionManager(eventLoop, config)
        , leaderRPC()
        , request()
        , response()
        , expResponse()
    {
        sessionManager.skipVerify = true;
        service = std::make_shared<RPC::ServiceMock>();
        server.reset(new RPC::Server(eventLoop,
                                     Protocol::Common::MAX_MESSAGE_LENGTH));
        RPC::Address address("127.0.0.1", Protocol::Common::DEFAULT_PORT);
        address.refresh(RPC::Address::TimePoint::max());
        EXPECT_EQ("", server->bind(address));
        server->registerService(Protocol::Common::ServiceId::CLIENT_SERVICE,
                                service, 1);
        leaderRPC.reset(new LeaderRPC(address,
                                      clusterUUID,
                                      sessionCreationBackoff,
                                      sessionManager));


        request.mutable_tree()->mutable_read()->set_path("foo");
        expResponse.mutable_tree()->set_status(Protocol::Client::Status::OK);
        expResponse.mutable_tree()->mutable_read()->set_contents("bar");
    }
    ~ClientLeaderRPCTest()
    {
        RPC::ClientSession::connectFn = ::connect;
        eventLoop.exit();
        if (eventLoopThread.joinable())
            eventLoopThread.join();
    }

    void init() {
        eventLoopThread = std::thread(&Event::Loop::runForever, &eventLoop);
    }

    Event::Loop eventLoop;
    Backoff sessionCreationBackoff;
    std::shared_ptr<RPC::ServiceMock> service;
    std::unique_ptr<RPC::Server> server;
    std::thread eventLoopThread;
    Core::Config config;
    SessionManager::ClusterUUID clusterUUID;
    SessionManager sessionManager;
    std::unique_ptr<LeaderRPC> leaderRPC;
    Protocol::Client::StateMachineQuery::Request request;
    Protocol::Client::StateMachineQuery::Response response;
    Protocol::Client::StateMachineQuery::Response expResponse;
};

// copied from RPC/ClientSessionTest.cc
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

TEST_F(ClientLeaderRPCTest, Call_start_timeout) {
    LeaderRPC::Call call(*leaderRPC);
    ConnectInProgress c;
    RPC::ClientSession::connectFn = std::ref(c);
    call.start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::min());
    EXPECT_EQ("Closed session: Failed to create session to leader: "
              "timeout expired",
              call.cachedSession->toString());
    EXPECT_EQ("Failed to create session to leader: timeout expired",
              call.rpc.getErrorMessage());
    EXPECT_EQ(LeaderRPCBase::Call::Status::TIMEOUT,
              call.wait(response, TimePoint::min()));
    EXPECT_FALSE(leaderRPC->leaderSession.get());
}

TEST_F(ClientLeaderRPCTest, CallOK) {
    init();
    service->reply(OpCode::STATE_MACHINE_QUERY, request, expResponse);
    std::unique_ptr<LeaderRPCBase::Call> call = leaderRPC->makeCall();
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::OK,
              call->wait(response, TimePoint::max()));
    EXPECT_EQ(expResponse, response);
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
}

TEST_F(ClientLeaderRPCTest, CallCanceled) {
    init();
    std::unique_ptr<LeaderRPCBase::Call> call = leaderRPC->makeCall();
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    call->cancel();

    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    call->cancel();
    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
}

TEST_F(ClientLeaderRPCTest, CallRPCFailed) {
    init();
    service->closeSession(OpCode::STATE_MACHINE_QUERY, request);
    service->reply(OpCode::STATE_MACHINE_QUERY, request, expResponse);
    std::unique_ptr<LeaderRPCBase::Call> call = leaderRPC->makeCall();
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::OK,
              call->wait(response, TimePoint::max()));
    EXPECT_EQ(expResponse, response);
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
}

TEST_F(ClientLeaderRPCTest, Call_wait_notLeader) {
    init();
    Protocol::Client::Error error;
    error.set_error_code(Protocol::Client::Error::NOT_LEADER);

    // 1. no hint
    service->serviceSpecificError(OpCode::STATE_MACHINE_QUERY, request, error);

    // 2. bad hint (wrong port)
    error.set_leader_hint("127.0.0.1:0");
    service->serviceSpecificError(OpCode::STATE_MACHINE_QUERY, request, error);

    // 3. ok, fine, let it through
    service->reply(OpCode::STATE_MACHINE_QUERY, request, expResponse);

    std::unique_ptr<LeaderRPCBase::Call> call = leaderRPC->makeCall();

    // 1. no hint
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);

    // 2. hint
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    EXPECT_EQ("127.0.0.1:0", leaderRPC->leaderHint);

    // 3. try bad hint (wrong port)
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::RETRY,
              call->wait(response, TimePoint::max()));
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);

    // 4. finally works
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::OK,
              call->wait(response, TimePoint::max()));
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
    EXPECT_EQ(expResponse, response);
}

TEST_F(ClientLeaderRPCTest, Call_wait_timeout) {
    std::unique_ptr<LeaderRPCBase::Call> call = leaderRPC->makeCall();
    call->start(OpCode::STATE_MACHINE_QUERY, request, TimePoint::max());
    EXPECT_EQ(LeaderRPCBase::Call::Status::TIMEOUT,
              call->wait(response, TimePoint::min()));
}

// constructor and destructor tested adequately in tests for call()

TEST_F(ClientLeaderRPCTest, callOK) {
    init();
    service->reply(OpCode::STATE_MACHINE_QUERY, request, expResponse);
    leaderRPC->call(OpCode::STATE_MACHINE_QUERY, request, response,
                    TimePoint::max());
    EXPECT_EQ(expResponse, response);
}

TEST_F(ClientLeaderRPCTest, callRPCFailed) {
    init();
    service->closeSession(OpCode::STATE_MACHINE_QUERY, request);
    service->reply(OpCode::STATE_MACHINE_QUERY, request, expResponse);
    leaderRPC->call(OpCode::STATE_MACHINE_QUERY, request, response,
                    TimePoint::max());
    EXPECT_EQ(expResponse, response);
}

TEST_F(ClientLeaderRPCTest, getSession_normal) {
    // first create the connection
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    EXPECT_EQ("Active session to 127.0.0.1 (resolved to 127.0.0.1:5254)",
              leaderRPC->getSession(TimePoint::max())->toString());
    EXPECT_TRUE(leaderRPC->leaderSession.get());

    // now return right away
    EXPECT_EQ("Active session to 127.0.0.1 (resolved to 127.0.0.1:5254)",
              leaderRPC->getSession(TimePoint::min())->toString());
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ(1U, leaderRPC->connected.notificationCount);
}

TEST_F(ClientLeaderRPCTest, getSession_timeoutWhileWaitingOnOther) {
    leaderRPC->isConnecting = true;
    EXPECT_EQ("Closed session: Failed to get session to leader in time that "
              "another thread is creating: timeout expired",
              leaderRPC->getSession(Clock::now() +
                                    std::chrono::milliseconds(1))->toString());
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    leaderRPC->isConnecting = false;
}

struct GetSessionHelper {
    explicit GetSessionHelper(LeaderRPC& leaderRPC)
        : leaderRPC(leaderRPC)
        , count(0)
    {
    }
    void operator()() {
        leaderRPC.isConnecting = false;
        ++count;
    }
    LeaderRPC& leaderRPC;
    uint64_t count;
};

TEST_F(ClientLeaderRPCTest, getSession_blockSlightlyOnOther) {
    // This simulates the case where one thread is connecting,
    // so getSession had to block for a while.
    leaderRPC->isConnecting = true;
    GetSessionHelper helper(*leaderRPC);
    leaderRPC->connected.callback = std::ref(helper);
    EXPECT_EQ("Active session to 127.0.0.1 (resolved to 127.0.0.1:5254)",
              leaderRPC->getSession(TimePoint::max())->toString());
    EXPECT_EQ(1U, helper.count);
}

TEST_F(ClientLeaderRPCTest, getSession_timeoutBeforeCreateSession) {
    leaderRPC->leaderHint = "127.0.0.1:5254";
    EXPECT_EQ("Closed session: Failed to create session to leader: "
              "timeout expired",
              leaderRPC->getSession(TimePoint::min())->toString());
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("127.0.0.1:5254", leaderRPC->leaderHint);
}

TEST_F(ClientLeaderRPCTest, reportFailure) {
    std::shared_ptr<RPC::ClientSession> session1 =
        leaderRPC->getSession(TimePoint::max());
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    leaderRPC->reportFailure(session1);
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    std::shared_ptr<RPC::ClientSession> session2 =
        leaderRPC->getSession(TimePoint::max());
    leaderRPC->reportFailure(session1);
    EXPECT_TRUE(leaderRPC->leaderSession.get());
}

TEST_F(ClientLeaderRPCTest, reportNotLeader) {
    std::shared_ptr<RPC::ClientSession> session1 =
        leaderRPC->getSession(TimePoint::max());
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    leaderRPC->reportNotLeader(session1);
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    std::shared_ptr<RPC::ClientSession> session2 =
        leaderRPC->getSession(TimePoint::max());
    leaderRPC->reportNotLeader(session1);
    EXPECT_TRUE(leaderRPC->leaderSession.get());
}

TEST_F(ClientLeaderRPCTest, reportRedirect) {
    std::shared_ptr<RPC::ClientSession> session1 =
        leaderRPC->getSession(TimePoint::max());
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
    leaderRPC->reportRedirect(session1, "127.0.0.1:0");
    EXPECT_FALSE(leaderRPC->leaderSession.get());
    EXPECT_EQ("127.0.0.1:0", leaderRPC->leaderHint);
    std::shared_ptr<RPC::ClientSession> session2 =
        leaderRPC->getSession(TimePoint::max());
    EXPECT_EQ("", leaderRPC->leaderHint);
    leaderRPC->reportRedirect(session1, "127.0.0.1:1");
    EXPECT_TRUE(leaderRPC->leaderSession.get());
    EXPECT_EQ("", leaderRPC->leaderHint);
}

} // namespace LogCabin::Client::<anonymous>
} // namespace LogCabin::Client
} // namespace LogCabin
