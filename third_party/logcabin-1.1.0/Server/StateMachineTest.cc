/* Copyright (c) 2013-2014 Stanford University
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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "build/Protocol/Raft.pb.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/StringUtil.h"
#include "Core/STLUtil.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"
#include "Server/StateMachine.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/MemoryLog.h"
#include "Storage/SnapshotFile.h"

namespace LogCabin {
namespace Server {
extern bool stateMachineSuppressThreads;
extern uint32_t stateMachineChildSleepMs;
namespace {

class ServerStateMachineTest : public ::testing::Test {
  public:
    ServerStateMachineTest()
      : globals()
      , consensus()
      , stateMachine()
      , timeMocker()
    {
        RaftConsensusInternal::startThreads = false;
        consensus.reset(new RaftConsensus(globals));
        consensus->serverId = 1;
        consensus->log.reset(new Storage::MemoryLog());
        consensus->storageLayout.initTemporary();

        Storage::Log::Entry entry;
        entry.set_term(1);
        entry.set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *entry.mutable_configuration() =
            Core::ProtoBuf::fromString<Protocol::Raft::Configuration>(
                "prev_configuration {"
                "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
                "}");
        consensus->init();
        consensus->append({&entry});
        consensus->startNewElection();
        consensus->configuration->localServer->lastSyncedIndex =
            consensus->log->getLastLogIndex();
        consensus->advanceCommitIndex();

        stateMachineSuppressThreads = true;
        stateMachine.reset(new StateMachine(consensus, globals.config,
                                            globals));
    }
    ~ServerStateMachineTest() {
        stateMachineSuppressThreads = false;
        stateMachineChildSleepMs = 0;
    }


    Core::Buffer
    serialize(const StateMachine::Command::Request& command) {
        Core::Buffer out;
        Core::ProtoBuf::serialize(command, out);
        return out;
    }

    Globals globals;
    std::shared_ptr<RaftConsensus> consensus;
    std::unique_ptr<StateMachine> stateMachine;
    Core::Time::SteadyClock::Mocker timeMocker;
};

TEST_F(ServerStateMachineTest, query_tree)
{
    StateMachine::Query::Request request;
    StateMachine::Query::Response response;
    auto& read = *request.mutable_tree()->mutable_read();
    read.set_path("/foo");
    EXPECT_TRUE(stateMachine->query(request, response));
    EXPECT_EQ(Protocol::Client::Status::LOOKUP_ERROR,
              response.tree().status());
}

TEST_F(ServerStateMachineTest, query_unknown)
{
    StateMachine::Query::Request request;
    StateMachine::Query::Response response;
    Core::Debug::setLogPolicy({{"", "ERROR"}});
    EXPECT_FALSE(stateMachine->query(request, response));
    EXPECT_FALSE(stateMachine->query(request, response));
}

struct WaitHelper {
    explicit WaitHelper(StateMachine& stateMachine)
        : stateMachine(stateMachine)
        , iter(0)
    {
    }
    void operator()() {
        ++iter;
        if (iter == 1) {
            EXPECT_EQ(0U, stateMachine.lastApplied);
            stateMachine.lastApplied = 2;
        } else if (iter == 2) {
            EXPECT_EQ(2U, stateMachine.lastApplied);
            stateMachine.lastApplied = 3;
        }
    }
    StateMachine& stateMachine;
    uint64_t iter;
};

TEST_F(ServerStateMachineTest, wait)
{
    WaitHelper helper(*stateMachine);
    stateMachine->entriesApplied.callback = std::ref(helper);
    stateMachine->wait(3);
    EXPECT_EQ(2U, helper.iter);
}

TEST_F(ServerStateMachineTest, waitForResponse_wait)
{
    StateMachine::Command::Request request;
    request.mutable_open_session();
    StateMachine::Command::Response response;
    WaitHelper helper(*stateMachine);
    stateMachine->entriesApplied.callback = std::ref(helper);
    EXPECT_TRUE(stateMachine->waitForResponse(3, request, response));
    EXPECT_EQ(2U, helper.iter);
}

TEST_F(ServerStateMachineTest, waitForResponse_tree)
{
    Core::Debug::setLogPolicy({{"Server/StateMachine.cc", "ERROR"}});
    stateMachine->sessions.insert({1, {}});
    StateMachine::Session& session = stateMachine->sessions.at(1);
    StateMachine::Command::Response r1;
    StateMachine::Command::Response r2;
    r1.mutable_tree()->set_status(Protocol::Client::Status::LOOKUP_ERROR);
    session.responses.insert({1, r1});

    StateMachine::Command::Request request;
    auto& exactlyOnce = *request.mutable_tree()->mutable_exactly_once();
    exactlyOnce.set_client_id(2);
    exactlyOnce.set_rpc_number(1);
    EXPECT_TRUE(stateMachine->waitForResponse(0, request, r2));
    EXPECT_EQ("tree { "
              "  status: SESSION_EXPIRED "
              "}", r2);

    exactlyOnce.set_client_id(1);
    exactlyOnce.set_rpc_number(2);
    EXPECT_TRUE(stateMachine->waitForResponse(0, request, r2));
    EXPECT_EQ("tree { "
              "  status: SESSION_EXPIRED "
              "}", r2);

    Core::Debug::setLogPolicy({{"", "WARNING"}});
    exactlyOnce.set_client_id(1);
    exactlyOnce.set_rpc_number(1);
    EXPECT_TRUE(stateMachine->waitForResponse(0, request, r2));
    EXPECT_EQ(r1, r2);
}

TEST_F(ServerStateMachineTest, waitForResponse_openSession)
{
    StateMachine::Command::Request request;
    request.mutable_open_session();
    StateMachine::Command::Response response;
    stateMachine->lastApplied = 3;
    EXPECT_TRUE(stateMachine->waitForResponse(3, request, response));
    EXPECT_EQ("open_session { "
              "  client_id: 3 "
              "}",
              response);
}

TEST_F(ServerStateMachineTest, waitForResponse_closeSession)
{
    stateMachine->lastApplied = 3;
    StateMachine::Command::Request request;
    request.mutable_close_session()->set_client_id(3);
    StateMachine::Command::Response response;
    stateMachine->versionHistory.insert({3, 2});
    EXPECT_FALSE(stateMachine->waitForResponse(2, request, response));
    EXPECT_FALSE(response.has_close_session());
    EXPECT_TRUE(stateMachine->waitForResponse(3, request, response));
    EXPECT_EQ("close_session { "
              "}",
              response);
}

TEST_F(ServerStateMachineTest, waitForResponse_advanceVersion)
{
    StateMachine::Command::Request request;
    request.mutable_advance_version()->
        set_requested_version(90);
    StateMachine::Command::Response response;
    stateMachine->lastApplied = 3;
    EXPECT_TRUE(stateMachine->waitForResponse(3, request, response));
    EXPECT_EQ("advance_version { "
              "  running_version: 1 "
              "}",
              response);
}

TEST_F(ServerStateMachineTest, waitForResponse_unknown)
{
    StateMachine::Command::Request request; // empty
    StateMachine::Command::Response response;
    stateMachine->lastApplied = 3;
    EXPECT_FALSE(stateMachine->waitForResponse(3, request, response));
    EXPECT_EQ("", response);
}

struct IsTakingSnapshotHelper {
    explicit IsTakingSnapshotHelper(StateMachine& stateMachine)
        : stateMachine(stateMachine)
        , count(0)
    {
    }
    void operator()() {
        std::function<void()> callback;
        std::swap(callback, stateMachine.mutex.callback);
        if (count == 1) {
            stateMachine.mutex.unlock();
            EXPECT_TRUE(stateMachine.isTakingSnapshot());
            stateMachine.mutex.lock();
        }
        std::swap(callback, stateMachine.mutex.callback);
        ++count;
    }
    StateMachine& stateMachine;
    uint64_t count;
};

TEST_F(ServerStateMachineTest, isTakingSnapshot)
{
    IsTakingSnapshotHelper helper(*stateMachine);
    EXPECT_FALSE(stateMachine->isTakingSnapshot());
    {
        std::unique_lock<Core::Mutex> lockGuard(stateMachine->mutex);
        stateMachine->mutex.callback = std::ref(helper);
        stateMachine->takeSnapshot(1, lockGuard);
    }
    EXPECT_FALSE(stateMachine->isTakingSnapshot());
}

struct StartTakingSnapshotHelper {
    explicit StartTakingSnapshotHelper(StateMachine& stateMachine)
        : stateMachine(stateMachine)
        , count(0)
    {
    }
    void operator()() {
        if (count == 0) {
            std::unique_lock<Core::Mutex> lockGuard(stateMachine.mutex);
            stateMachine.takeSnapshot(1, lockGuard);
        }
        ++count;

    }
    StateMachine& stateMachine;
    uint64_t count;
};

TEST_F(ServerStateMachineTest, startTakingSnapshot)
{
    EXPECT_FALSE(stateMachine->isSnapshotRequested);
    EXPECT_EQ(0U, stateMachine->snapshotSuggested.notificationCount);
    StartTakingSnapshotHelper helper(*stateMachine);
    stateMachine->snapshotStarted.callback = std::ref(helper);
    stateMachine->startTakingSnapshot();
    EXPECT_TRUE(stateMachine->isSnapshotRequested);
    EXPECT_EQ(1U, stateMachine->snapshotSuggested.notificationCount);
}

TEST_F(ServerStateMachineTest, startTakingSnapshot_alreadyStarted)
{
    stateMachine->childPid = 1000;
    stateMachine->startTakingSnapshot();
    EXPECT_FALSE(stateMachine->isSnapshotRequested);
    EXPECT_EQ(0U, stateMachine->snapshotSuggested.notificationCount);
    stateMachine->childPid = 0;
}

struct StopTakingSnapshotHelper {
    explicit StopTakingSnapshotHelper(StateMachine& stateMachine)
        : stateMachine(stateMachine)
        , count(0)
    {
    }
    void operator()() {
        if (count == 3) {
            int status = 0;
            EXPECT_EQ(stateMachine.childPid,
                      waitpid(stateMachine.childPid, &status, 0))
                << strerror(errno);
            EXPECT_TRUE(WIFSIGNALED(status));
            EXPECT_EQ(SIGTERM, WTERMSIG(status));
            stateMachine.childPid = 0;
        }
        ++count;
    }
    StateMachine& stateMachine;
    uint64_t count;
};

TEST_F(ServerStateMachineTest, stopTakingSnapshot)
{
    // start a snapshot
    errno = 0;
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << strerror(errno); // error
    if (pid == 0) { // child
        stateMachine->globals.unblockAllSignals();
        while (true)
            usleep(5000);
    }
    // parent continues here
    stateMachine->childPid = pid;
    StopTakingSnapshotHelper helper(*stateMachine);
    stateMachine->snapshotCompleted.callback = std::ref(helper);
    stateMachine->stopTakingSnapshot();
    EXPECT_EQ(4U, helper.count);
}

TEST_F(ServerStateMachineTest, stopTakingSnapshot_noSnapshot)
{
    stateMachine->stopTakingSnapshot();
}

TEST_F(ServerStateMachineTest, getInhibit)
{
    // time is mocked
    EXPECT_EQ(std::chrono::nanoseconds::zero(),
              stateMachine->getInhibit());
    stateMachine->setInhibit(std::chrono::nanoseconds(0));
    EXPECT_EQ(std::chrono::nanoseconds::zero(),
              stateMachine->getInhibit());
    stateMachine->setInhibit(std::chrono::nanoseconds(1));
    EXPECT_EQ(std::chrono::nanoseconds(1),
              stateMachine->getInhibit());
    stateMachine->setInhibit(std::chrono::seconds(10000));
    EXPECT_EQ(std::chrono::seconds(10000),
              stateMachine->getInhibit());
}

TEST_F(ServerStateMachineTest, setInhibit)
{
    stateMachine->setInhibit(std::chrono::nanoseconds::zero());
    EXPECT_EQ(std::chrono::nanoseconds::zero(),
              stateMachine->getInhibit());
    stateMachine->setInhibit(std::chrono::nanoseconds(100));
    EXPECT_EQ(std::chrono::nanoseconds(100),
              stateMachine->getInhibit());
    stateMachine->setInhibit(std::chrono::nanoseconds(-100));
    EXPECT_EQ(std::chrono::nanoseconds::zero(),
              stateMachine->getInhibit());
    stateMachine->setInhibit(std::chrono::nanoseconds::max());
    EXPECT_LT(std::chrono::seconds(60L * 60L * 24L * 365L * 100L),
              stateMachine->getInhibit());
    EXPECT_LT(0U, stateMachine->snapshotSuggested.notificationCount);
}

TEST_F(ServerStateMachineTest, apply_tree)
{
    stateMachine->sessionTimeoutNanos = 1;
    RaftConsensus::Entry entry;
    entry.index = 6;
    entry.type = RaftConsensus::Entry::DATA;
    entry.clusterTime = 2;
    StateMachine::Command::Request command =
        Core::ProtoBuf::fromString<StateMachine::Command::Request>(
            "tree: { "
            " exactly_once: { "
            "  client_id: 39 "
            "  first_outstanding_rpc: 2 "
            "  rpc_number: 3 "
            " } "
            " make_directory { "
            "  path: '/a' "
            " } "
            "}");
    entry.command = serialize(command);
    std::vector<std::string> children;

    // session does not exist
    stateMachine->sessions.insert({1, {}});
    stateMachine->apply(entry);
    stateMachine->expireSessions(entry.clusterTime);
    stateMachine->tree.listDirectory("/", children);
    EXPECT_EQ((std::vector<std::string> {}), children);
    ASSERT_EQ(0U, stateMachine->sessions.size());

    // session exists and need to apply
    stateMachine->sessions.insert({1, {}});
    stateMachine->sessions.insert({39, {}});
    stateMachine->apply(entry);
    stateMachine->expireSessions(entry.clusterTime);
    stateMachine->tree.listDirectory("/", children);
    EXPECT_EQ((std::vector<std::string> {"a/"}), children);
    ASSERT_EQ(1U, stateMachine->sessions.size());
    EXPECT_EQ(2U, stateMachine->sessions.at(39).lastModified);

    // session exists and response exists
    stateMachine->sessions.insert({1, {}});
    stateMachine->tree.removeDirectory("/a");
    stateMachine->apply(entry);
    stateMachine->expireSessions(entry.clusterTime);
    stateMachine->tree.listDirectory("/", children);
    EXPECT_EQ((std::vector<std::string> {}), children);
    ASSERT_EQ(1U, stateMachine->sessions.size());
    EXPECT_EQ(2U, stateMachine->sessions.at(39).lastModified);

    // session exists but response discarded
    stateMachine->sessions.insert({1, {}});
    stateMachine->expireResponses(stateMachine->sessions.at(39), 4);
    stateMachine->apply(entry);
    stateMachine->expireSessions(entry.clusterTime);
    stateMachine->tree.listDirectory("/", children);
    EXPECT_EQ((std::vector<std::string> {}), children);
    ASSERT_EQ(1U, stateMachine->sessions.size());
    EXPECT_EQ(2U, stateMachine->sessions.at(39).lastModified);
}

TEST_F(ServerStateMachineTest, apply_openSession)
{
    stateMachine->sessionTimeoutNanos = 1;
    stateMachine->sessions.insert({1, {}});
    StateMachine::Command::Request command =
        Core::ProtoBuf::fromString<StateMachine::Command::Request>(
            "open_session: {}");
    RaftConsensus::Entry entry;
    entry.index = 6;
    entry.type = RaftConsensus::Entry::DATA;
    entry.command = serialize(command);
    entry.clusterTime = 2;

    stateMachine->apply(entry);
    stateMachine->expireSessions(entry.clusterTime);
    ASSERT_EQ((std::vector<uint64_t>{6U}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(stateMachine->sessions)));
    StateMachine::Session& session = stateMachine->sessions.at(6);
    EXPECT_EQ(2U, session.lastModified);
    EXPECT_EQ(0U, session.firstOutstandingRPC);
    EXPECT_EQ(0U, session.responses.size());
}

TEST_F(ServerStateMachineTest, apply_closeSession)
{
    stateMachine->sessions.insert({2, {}});
    stateMachine->sessions.insert({3, {}});
    stateMachine->sessions.insert({4, {}});
    StateMachine::Command::Request command;
    command.mutable_close_session()->set_client_id(3);

    RaftConsensus::Entry entry;
    entry.index = 6;
    entry.type = RaftConsensus::Entry::DATA;
    entry.command = serialize(command);
    entry.clusterTime = 2;


    // first apply will have no effect (only warning) because state machine
    // version 1 does not support the CloseSession command
    Core::Debug::setLogPolicy({
        {"Server/StateMachine.cc", "ERROR"},
        {"", "WARNING"},
    });
    stateMachine->versionHistory.insert({4, 1});
    stateMachine->apply(entry);
    ASSERT_EQ((std::vector<uint64_t>{2, 3, 4}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(stateMachine->sessions)));
    Core::Debug::setLogPolicy({
        {"", "WARNING"},
    });

    // second apply will work
    stateMachine->versionHistory.insert({5, 2});
    stateMachine->apply(entry);
    ASSERT_EQ((std::vector<uint64_t>{2, 4}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(stateMachine->sessions)));

    // third apply will have no effect since the session was already closed
    stateMachine->apply(entry);
    ASSERT_EQ((std::vector<uint64_t>{2, 4}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(stateMachine->sessions)));
}


TEST_F(ServerStateMachineTest, apply_advanceVersion)
{
    RaftConsensus::Entry entry;
    entry.index = 6;
    entry.type = RaftConsensus::Entry::DATA;
    entry.clusterTime = 2;

    // stay at version 1
    StateMachine::Command::Request command;
    command.mutable_advance_version()->set_requested_version(1);
    entry.command = serialize(command);
    stateMachine->apply(entry);
    stateMachine->apply(entry);
    stateMachine->apply(entry); // should silently succeed
    EXPECT_EQ(1U, stateMachine->getVersion(10000));

    // up to version 2
    command.mutable_advance_version()->set_requested_version(2);
    entry.command = serialize(command);
    stateMachine->apply(entry);
    EXPECT_EQ(2U, stateMachine->getVersion(10000));

    // downgrade to version 1 should give warning
    command.mutable_advance_version()->set_requested_version(1);
    entry.command = serialize(command);
    Core::Debug::setLogPolicy({
        {"Server/StateMachine.cc", "ERROR"},
        {"", "WARNING"},
    });
    stateMachine->apply(entry);
    Core::Debug::setLogPolicy({
        {"", "WARNING"},
    });
    EXPECT_EQ(2U, stateMachine->getVersion(10000));
}

TEST_F(ServerStateMachineTest, apply_unknown)
{
    StateMachine::Command::Request command =
        Core::ProtoBuf::fromString<StateMachine::Command::Request>("");
    RaftConsensus::Entry entry;
    entry.index = 6;
    entry.type = RaftConsensus::Entry::DATA;
    entry.clusterTime = 2;
    entry.command = serialize(command);
    // should be no-op, definitely shouldn't panic, expect warning
    Core::Debug::setLogPolicy({
        {"Server/StateMachine.cc", "ERROR"},
        {"", "WARNING"},
    });
    stateMachine->apply(entry);
    stateMachine->apply(entry);
}

// This tries to test the use of kill() to stop a snapshotting child and exit
// quickly.
TEST_F(ServerStateMachineTest, applyThreadMain_exiting_TimingSensitive)
{
    // instruct the child process to sleep for 10s
    stateMachineChildSleepMs = 10000;
    consensus->exit();
    {
        // applyThread won't be able to kill() yet due to mutex
        std::unique_lock<Core::Mutex> lockGuard(stateMachine->mutex);
        stateMachine->applyThread = std::thread(&StateMachine::applyThreadMain,
                                                stateMachine.get());
        struct timeval startTime;
        EXPECT_EQ(0, gettimeofday(&startTime, NULL));
        stateMachine->takeSnapshot(1, lockGuard);
        struct timeval endTime;
        EXPECT_EQ(0, gettimeofday(&endTime, NULL));
        uint64_t elapsedMillis = uint64_t(
            ((endTime.tv_sec   * 1000 * 1000 + endTime.tv_usec) -
             (startTime.tv_sec * 1000 * 1000 + startTime.tv_usec)) / 1000);
        EXPECT_GT(200U, elapsedMillis) <<
            "This test depends on timing, so failures are likely under "
            "heavy load, valgrind, etc.";
    }
    EXPECT_EQ(0U, consensus->lastSnapshotIndex);
}

TEST_F(ServerStateMachineTest, serializeSessions)
{
    StateMachine::Command::Response r1;
    r1.mutable_tree()->set_status(Protocol::Client::Status::LOOKUP_ERROR);

    StateMachine::Command::Response r2;
    r2.mutable_tree()->set_status(Protocol::Client::Status::TYPE_ERROR);

    StateMachine::Session s1;
    s1.lastModified = 6;
    s1.firstOutstandingRPC = 5;
    s1.responses.insert({5, r1});
    s1.responses.insert({7, r2});
    stateMachine->sessions.insert({4, s1});

    StateMachine::Session s2;
    s2.firstOutstandingRPC = 9;
    s2.responses.insert({10, r2});
    s2.responses.insert({11, r1});
    stateMachine->sessions.insert({80, s2});

    StateMachine::Session s3;
    s3.firstOutstandingRPC = 6;
    stateMachine->sessions.insert({91, s3});

    SnapshotStateMachine::Header header;
    stateMachine->serializeSessions(header);

    stateMachine->sessions.at(80).responses.at(10) = r1;
    stateMachine->sessions.at(80).firstOutstandingRPC = 10;

    stateMachine->loadSessions(header);

    EXPECT_EQ((std::vector<std::uint64_t>{4, 80, 91}),
              Core::STLUtil::sorted(
                Core::STLUtil::getKeys(stateMachine->sessions)));
    EXPECT_EQ(6U, stateMachine->sessions.at(4).lastModified);
    EXPECT_EQ(5U, stateMachine->sessions.at(4).firstOutstandingRPC);
    EXPECT_EQ(9U, stateMachine->sessions.at(80).firstOutstandingRPC);
    EXPECT_EQ(6U, stateMachine->sessions.at(91).firstOutstandingRPC);
    EXPECT_EQ((std::vector<std::uint64_t>{5, 7}),
              Core::STLUtil::sorted(
                Core::STLUtil::getKeys(
                    stateMachine->sessions.at(4).responses)));
    EXPECT_EQ(r1, stateMachine->sessions.at(4).responses.at(5));
    EXPECT_EQ(r2, stateMachine->sessions.at(4).responses.at(7));
    EXPECT_EQ((std::vector<std::uint64_t>{10, 11}),
              Core::STLUtil::sorted(
                Core::STLUtil::getKeys(
                    stateMachine->sessions.at(80).responses)));
    EXPECT_EQ(r2, stateMachine->sessions.at(80).responses.at(10));
    EXPECT_EQ(r1, stateMachine->sessions.at(80).responses.at(11));
    EXPECT_EQ((std::vector<std::uint64_t>{}),
              Core::STLUtil::sorted(
                Core::STLUtil::getKeys(
                    stateMachine->sessions.at(91).responses)));
}

TEST_F(ServerStateMachineTest, serializeVersionHistory)
{
    // since MAX_SUPPORTED_VERSION is 1, these values all have to be 1 for now
    stateMachine->versionHistory[1] = 1;
    stateMachine->versionHistory[3] = 1;
    SnapshotStateMachine::Header header;
    stateMachine->serializeVersionHistory(header);
    stateMachine->versionHistory.erase(3);
    stateMachine->versionHistory[5] = 1;
    stateMachine->loadVersionHistory(header);
    EXPECT_EQ((std::map<uint64_t, uint16_t> {
                   {0, 1},
                   {1, 1},
                   {3, 1},
               }),
              stateMachine->versionHistory);
}

TEST_F(ServerStateMachineTest, expireResponses)
{
    stateMachine->sessions.insert({1, {}});
    StateMachine::Session& session = stateMachine->sessions.at(1);
    session.responses.insert({1, {}});
    session.responses.insert({2, {}});
    session.responses.insert({4, {}});
    session.responses.insert({5, {}});
    stateMachine->expireResponses(session, 4);
    stateMachine->expireResponses(session, 3);
    EXPECT_EQ(4U, session.firstOutstandingRPC);
    EXPECT_EQ((std::vector<uint64_t>{4U, 5U}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(session.responses)));
}

TEST_F(ServerStateMachineTest, expireSessions)
{
    stateMachine->sessionTimeoutNanos = 1;
    stateMachine->sessions.insert({1, {}});
    stateMachine->sessions.at(1).lastModified = 100;
    stateMachine->sessions.insert({2, {}});
    stateMachine->sessions.at(2).lastModified = 400;
    stateMachine->sessions.insert({3, {}});
    stateMachine->sessions.at(3).lastModified = 200;
    stateMachine->sessions.insert({4, {}});
    stateMachine->sessions.at(4).lastModified = 201;
    stateMachine->sessions.insert({5, {}});
    stateMachine->expireSessions(202);
    EXPECT_EQ((std::vector<uint64_t>{2U, 4U}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(stateMachine->sessions)));
}

TEST_F(ServerStateMachineTest, getVersion)
{
    EXPECT_EQ(1U, stateMachine->getVersion(0));
    EXPECT_EQ(1U, stateMachine->getVersion(1));
    EXPECT_EQ(1U, stateMachine->getVersion(10));

    stateMachine->versionHistory[2] = 50;
    stateMachine->versionHistory[3] = 60;
    stateMachine->versionHistory[6] = 90;

    EXPECT_EQ(1U, stateMachine->getVersion(0));
    EXPECT_EQ(1U, stateMachine->getVersion(1));
    EXPECT_EQ(50U, stateMachine->getVersion(2));
    EXPECT_EQ(60U, stateMachine->getVersion(3));
    EXPECT_EQ(60U, stateMachine->getVersion(4));
    EXPECT_EQ(60U, stateMachine->getVersion(5));
    EXPECT_EQ(90U, stateMachine->getVersion(6));
    EXPECT_EQ(90U, stateMachine->getVersion(7));
}

// loadSessions tested along with serializeSessions above

// loadSnapshot normal path tested along with takeSnapshot below

TEST_F(ServerStateMachineTest, loadSnapshot_empty)
{
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(1);
    writer->save();
    consensus->readSnapshot();
    EXPECT_DEATH(stateMachine->loadSnapshot(*consensus->snapshotReader),
                 "no format version field");
}

TEST_F(ServerStateMachineTest, loadSnapshot_unknownFormatVersion)
{
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(1);
    uint8_t formatVersion = 2;
    writer->writeRaw(&formatVersion, sizeof(formatVersion));
    writer->save();
    consensus->readSnapshot();
    EXPECT_DEATH(stateMachine->loadSnapshot(*consensus->snapshotReader),
                 "Snapshot contents format version read was 2, but this code "
                 "can only read version 1");
}

// loadVersionHistory normal path tested along with serializeVersionHistory
// above

TEST_F(ServerStateMachineTest, loadVersionHistory_unknownVersion)
{
    stateMachine->versionHistory.insert({1, 3});
    SnapshotStateMachine::Header header;
    stateMachine->serializeVersionHistory(header);
    EXPECT_DEATH(stateMachine->loadVersionHistory(header),
                 "State machine version read from snapshot was 3, but this "
                 "code only supports 1 through 2");
}

struct SnapshotThreadMainHelper {
    explicit SnapshotThreadMainHelper(StateMachine& stateMachine)
        : stateMachine(stateMachine)
        , count(0)
    {
    }
    void operator()() {
        // append a new entry every iteration so that shouldTakeSnapshot can
        // return true
        Storage::Log::Entry entry;
        entry.set_term(1);
        entry.set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *entry.mutable_configuration() =
            Core::ProtoBuf::fromString<Protocol::Raft::Configuration>(
                "prev_configuration {"
                "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
                "}");
        stateMachine.consensus->append({&entry});
        stateMachine.consensus->commitIndex =
            stateMachine.consensus->log->getLastLogIndex();
        stateMachine.lastApplied = stateMachine.consensus->commitIndex;

        if (count == 0) {
            // not inhibited, shouldn't take snapshot, no snapshot requested:
            // slept
            EXPECT_EQ(0U, stateMachine.numSnapshotsAttempted);

            stateMachine.setInhibit(std::chrono::nanoseconds(1));
            stateMachine.isSnapshotRequested = true;
            EXPECT_FALSE(
                    stateMachine.shouldTakeSnapshot(stateMachine.lastApplied));
        } else if (count == 1) {
            // inhibited and snapshot requested:
            // took snapshot
            EXPECT_EQ(1U, stateMachine.numSnapshotsAttempted);
            EXPECT_FALSE(stateMachine.isSnapshotRequested);

            EXPECT_FALSE(
                    stateMachine.shouldTakeSnapshot(stateMachine.lastApplied));
            stateMachine.snapshotMinLogSize = 1U;
            stateMachine.snapshotRatio = 0U;
            EXPECT_TRUE(
                    stateMachine.shouldTakeSnapshot(stateMachine.lastApplied));
        } else if (count == 2) {
            // inhibited, should take snapshot, and no snapshot requested:
            // slept
            EXPECT_EQ(1U, stateMachine.numSnapshotsAttempted);

            EXPECT_TRUE(
                    stateMachine.shouldTakeSnapshot(stateMachine.lastApplied));
            stateMachine.setInhibit(std::chrono::nanoseconds(0));
        } else if (count == 3) {
            // not inhibited, should take snapshot, and no snapshot requested:
            // took snapshot
            EXPECT_EQ(2U, stateMachine.numSnapshotsAttempted);

            stateMachine.exiting = true;
        }
        ++count;

    }
    StateMachine& stateMachine;
    uint64_t count;
};

TEST_F(ServerStateMachineTest, snapshotThreadMain)
{
    // time is mocked
    stateMachine->lastApplied = 1;
    SnapshotThreadMainHelper helper(*stateMachine);
    stateMachine->snapshotSuggested.callback = std::ref(helper);
    stateMachine->snapshotThreadMain();
    EXPECT_EQ(4U, helper.count);
}


struct SnapshotWatchdogThreadMainHelper {
    explicit SnapshotWatchdogThreadMainHelper(StateMachine& stateMachine)
        : count(0)
        , stateMachine(stateMachine)
    {
    }
    void operator()() {
        if (count == 0) {
            // no snapshot, do nothing
        } else if (count == 1) {
            // no snapshot still
            // start a snapshot
            errno = 0;
            pid_t pid = fork();
            ASSERT_NE(pid, -1) << strerror(errno); // error
            if (pid == 0) { // child
                stateMachine.globals.unblockAllSignals();
                while (true)
                    usleep(5000);
            }
            // parent continues here
            stateMachine.childPid = pid;
            stateMachine.writer.reset(
                new Storage::SnapshotFile::Writer(
                    stateMachine.consensus->storageLayout));
            stateMachine.numSnapshotsAttempted = 1;
        } else if (count == 2) {
            // should be tracking now
            Core::Time::SteadyClock::mockValue +=
                std::chrono::seconds(8);
        } else if (count == 3) {
            // spurious wakeup
            // make some progress
            *stateMachine.writer->sharedBytesWritten.value += 1;
            Core::Time::SteadyClock::mockValue +=
                std::chrono::seconds(3);
        } else if (count == 4) {
            // check passed
            // now don't make progress
            Core::Time::SteadyClock::mockValue +=
                std::chrono::seconds(11);
            int status = 0;
            EXPECT_EQ(0, // still running
                      waitpid(stateMachine.childPid, &status, WNOHANG))
                << status << strerror(errno);
            Core::Debug::setLogPolicy({
                {"Server/StateMachine.cc", "SILENT"},
                {"", "WARNING"},
            });
        } else if (count == 5) {
            Core::Debug::setLogPolicy({
                {"", "WARNING"},
            });
            // child should be receiving SIGKILL
            int status = 0;
            EXPECT_EQ(stateMachine.childPid,
                      waitpid(stateMachine.childPid, &status, 0))
                << strerror(errno);
            EXPECT_TRUE(WIFSIGNALED(status));
            EXPECT_EQ(SIGKILL, WTERMSIG(status));
            stateMachine.childPid = 0;
            stateMachine.writer->discard();
            stateMachine.writer.reset();
            stateMachine.numSnapshotsFailed = 1;
        } else if (count == 6) {
            // no more child process
            stateMachine.exiting = true;
        }
        ++count;

    }
    uint64_t count;
    StateMachine& stateMachine;
};

TEST_F(ServerStateMachineTest, snapshotWatchdogThreadMain)
{
    Core::Time::SteadyClock::mockValue =
        Core::Time::SteadyClock::time_point();
    SnapshotWatchdogThreadMainHelper helper(*stateMachine);
    stateMachine->snapshotStarted.callback = std::ref(helper);
    stateMachine->snapshotWatchdogThreadMain();
    EXPECT_EQ(7U, helper.count);
}

TEST_F(ServerStateMachineTest, takeSnapshot)
{
    EXPECT_EQ(0U, consensus->lastSnapshotIndex);
    stateMachine->tree.makeDirectory("/foo");
    stateMachine->sessions.insert({4, {}});
    {
        std::unique_lock<Core::Mutex> lockGuard(stateMachine->mutex);
        stateMachine->takeSnapshot(1, lockGuard);
    }
    stateMachine->tree.removeDirectory("/foo");
    stateMachine->sessions.clear();
    EXPECT_EQ(1U, consensus->lastSnapshotIndex);
    consensus->discardUnneededEntries();
    consensus->readSnapshot();
    stateMachine->loadSnapshot(*consensus->snapshotReader);
    std::vector<std::string> children;
    stateMachine->tree.listDirectory("/", children);
    EXPECT_EQ((std::vector<std::string>{"foo/"}), children);
    EXPECT_EQ((std::vector<std::uint64_t>{4}),
              Core::STLUtil::sorted(
                  Core::STLUtil::getKeys(stateMachine->sessions)));
}

} // namespace LogCabin::Server::<anonymous>
} // namespace LogCabin::Server
} // namespace LogCabin

