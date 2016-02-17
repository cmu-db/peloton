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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include "build/Protocol/Raft.pb.h"
#include "Core/ProtoBuf.h"
#include "Core/STLUtil.h"
#include "Core/StringUtil.h"
#include "Core/Time.h"
#include "Core/Util.h"
#include "Protocol/Common.h"
#include "RPC/Address.h"
#include "RPC/ServiceMock.h"
#include "RPC/Server.h"
#include "Server/RaftConsensus.h"
#include "Server/Globals.h"
#include "Storage/MemoryLog.h"
#include "Storage/SnapshotFile.h"
#include "include/LogCabin/Debug.h"

namespace LogCabin {
namespace Server {
namespace {

using namespace RaftConsensusInternal; // NOLINT
typedef RaftConsensus::State State;
typedef RaftConsensus::ClientResult ClientResult;
typedef Storage::Log Log;
using std::chrono::milliseconds;

// class Server: nothing to test

// class LocalServer: nothing to test

// class Peer: TODO(ongaro): low-priority tests
// see also regression_nextIndexForNewServer

// class SimpleConfiguration

bool
idHeart(Server& server)
{
    return server.serverId < 3;
}

void
setAddr(Server& server)
{
    using Core::StringUtil::format;
    server.addresses = format("server%lu", server.serverId);
}

uint64_t
getServerId(Server& server)
{
    return server.serverId;
}

Protocol::Raft::Configuration desc(const std::string& description) {
    using Core::ProtoBuf::fromString;
    return fromString<Protocol::Raft::Configuration>(description);
}

Protocol::Raft::SimpleConfiguration sdesc(const std::string& description) {
    using Core::ProtoBuf::fromString;
    return fromString<Protocol::Raft::SimpleConfiguration>(description);
}

/**
 * Custom ServiceMock handler that increases Raft's currentTerm before
 * responding to a request.
 */
class BumpTermAndReply : public RPC::ServiceMock::Handler {
    explicit BumpTermAndReply(RaftConsensus& consensus,
                              const google::protobuf::Message& response)
        : consensus(consensus)
        , response(Core::ProtoBuf::copy(response)) {
    }
    void handleRPC(RPC::ServerRPC serverRPC) {
        // Avoid using stepDown() since it calls interruptAll() which cancels
        // this RPC!
        ++consensus.currentTerm;
        consensus.leaderId = 0;
        consensus.votedFor = 0;
        consensus.updateLogMetadata();
        consensus.state = State::FOLLOWER;
        consensus.setElectionTimer();
        consensus.stateChanged.notify_all();
        serverRPC.reply(*response);
    }
    RaftConsensus& consensus;
    std::unique_ptr<google::protobuf::Message> response;
};


class ServerRaftConsensusSimpleConfigurationTest : public ::testing::Test {
    ServerRaftConsensusSimpleConfigurationTest()
        : globals()
        , consensus(globals)
        , cfg()
        , emptyCfg()
        , oneCfg()
    {
        consensus.log.reset(new Storage::MemoryLog());
        startThreads = false;
        cfg.servers = {
            makeServer(1),
            makeServer(2),
            makeServer(3),
        };
        oneCfg.servers = {
            makeServer(1),
        };
    }
    ~ServerRaftConsensusSimpleConfigurationTest()
    {
        startThreads = true;
    }

    std::shared_ptr<Server> makeServer(uint64_t serverId) {
        return std::shared_ptr<Server>(new Peer(serverId, consensus));
    }

    Globals globals;
    RaftConsensus consensus;
    Configuration::SimpleConfiguration cfg;
    Configuration::SimpleConfiguration emptyCfg;
    Configuration::SimpleConfiguration oneCfg;
};

TEST_F(ServerRaftConsensusSimpleConfigurationTest, all) {
    EXPECT_TRUE(emptyCfg.all(idHeart));
    EXPECT_FALSE(cfg.all(idHeart));
    cfg.servers.pop_back();
    EXPECT_TRUE(cfg.all(idHeart));
}

TEST_F(ServerRaftConsensusSimpleConfigurationTest, contains) {
    std::shared_ptr<Server> s = cfg.servers.back();
    EXPECT_FALSE(emptyCfg.contains(s));
    EXPECT_TRUE(cfg.contains(s));
    cfg.servers.pop_back();
    EXPECT_FALSE(cfg.contains(s));
}


TEST_F(ServerRaftConsensusSimpleConfigurationTest, forEach) {
    cfg.forEach(setAddr);
    emptyCfg.forEach(setAddr);
    EXPECT_EQ("server1", cfg.servers.at(0)->addresses);
    EXPECT_EQ("server2", cfg.servers.at(1)->addresses);
    EXPECT_EQ("server3", cfg.servers.at(2)->addresses);
}

TEST_F(ServerRaftConsensusSimpleConfigurationTest, min) {
    EXPECT_EQ(0U, emptyCfg.min(getServerId));
    EXPECT_EQ(1U, oneCfg.min(getServerId));
    EXPECT_EQ(1U, cfg.min(getServerId));
}

TEST_F(ServerRaftConsensusSimpleConfigurationTest, quorumAll) {
    EXPECT_TRUE(emptyCfg.quorumAll(idHeart));
    EXPECT_TRUE(oneCfg.all(idHeart));
    EXPECT_TRUE(cfg.quorumAll(idHeart));
    cfg.servers.push_back(makeServer(4));
    EXPECT_FALSE(cfg.quorumAll(idHeart));
}

TEST_F(ServerRaftConsensusSimpleConfigurationTest, quorumMin) {
    EXPECT_EQ(0U, emptyCfg.quorumMin(getServerId));
    EXPECT_EQ(1U, oneCfg.quorumMin(getServerId));
    EXPECT_EQ(2U, cfg.quorumMin(getServerId));
    cfg.servers.pop_back();
    EXPECT_EQ(1U, cfg.quorumMin(getServerId));
}

class ServerRaftConsensusConfigurationTest
            : public ServerRaftConsensusSimpleConfigurationTest {
    ServerRaftConsensusConfigurationTest()
        : cfg(1, consensus)
    {
    }
    Configuration cfg;
};

TEST_F(ServerRaftConsensusConfigurationTest, forEach) {
    cfg.forEach(setAddr);
    EXPECT_EQ("server1", cfg.localServer->addresses);
}

TEST_F(ServerRaftConsensusConfigurationTest, hasVote) {
    auto s2 = makeServer(2);
    EXPECT_FALSE(cfg.hasVote(cfg.localServer));
    EXPECT_FALSE(cfg.hasVote(s2));
    cfg.oldServers.servers.push_back(cfg.localServer);
    cfg.newServers.servers.push_back(s2);
    cfg.state = Configuration::State::STABLE;
    EXPECT_TRUE(cfg.hasVote(cfg.localServer));
    EXPECT_FALSE(cfg.hasVote(s2));
    cfg.state = Configuration::State::TRANSITIONAL;
    EXPECT_TRUE(cfg.hasVote(cfg.localServer));
    EXPECT_TRUE(cfg.hasVote(s2));
    cfg.state = Configuration::State::STAGING;
    EXPECT_TRUE(cfg.hasVote(cfg.localServer));
    EXPECT_FALSE(cfg.hasVote(s2));
}

TEST_F(ServerRaftConsensusConfigurationTest, quorumAll) {
    // TODO(ongaro): low-priority test
}

TEST_F(ServerRaftConsensusConfigurationTest, quorumMin) {
    // TODO(ongaro): low-priority test
}

// resetStagingServers tested at bottom of setStagingServers test

const char* d =
    "prev_configuration {"
    "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
    "}";

const char* d2 =
    "prev_configuration {"
    "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
    "}"
    "next_configuration {"
        "servers { server_id: 1, addresses: '127.0.0.1:5256' }"
    "}";

const char* d3 =
    "prev_configuration {"
    "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
    "    servers { server_id: 2, addresses: '127.0.0.1:5255' }"
    "}";

const char* d4 =
    "prev_configuration {"
    "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
    "}"
    "next_configuration {"
        "servers { server_id: 2, addresses: '127.0.0.1:5255' }"
    "}";

TEST_F(ServerRaftConsensusConfigurationTest, reset) {
    std::string expected = Core::StringUtil::toString(cfg);
    cfg.setConfiguration(1, desc(d4));
    std::shared_ptr<Server> s2 = cfg.getServer(2);
    cfg.reset();
    EXPECT_EQ(expected, Core::StringUtil::toString(cfg));
    EXPECT_EQ(0U, cfg.oldServers.servers.size());
    EXPECT_EQ(0U, cfg.newServers.servers.size());
    EXPECT_TRUE(dynamic_cast<Peer*>(s2.get())->exiting);
}

TEST_F(ServerRaftConsensusConfigurationTest, setConfiguration) {
    cfg.setConfiguration(1, desc(d));
    EXPECT_EQ(Configuration::State::STABLE, cfg.state);
    EXPECT_EQ(1U, cfg.id);
    EXPECT_EQ(d, cfg.description);
    EXPECT_EQ(1U, cfg.oldServers.servers.size());
    EXPECT_EQ(0U, cfg.newServers.servers.size());
    EXPECT_EQ("127.0.0.1:5254", cfg.oldServers.servers.at(0)->addresses);
    EXPECT_EQ(1U, cfg.knownServers.size());

    cfg.setConfiguration(2, desc(d2));
    EXPECT_EQ(Configuration::State::TRANSITIONAL, cfg.state);
    EXPECT_EQ(2U, cfg.id);
    EXPECT_EQ(d2, cfg.description);
    EXPECT_EQ(1U, cfg.oldServers.servers.size());
    EXPECT_EQ(1U, cfg.newServers.servers.size());
    EXPECT_EQ("127.0.0.1:5256", cfg.oldServers.servers.at(0)->addresses);
    EXPECT_EQ("127.0.0.1:5256", cfg.newServers.servers.at(0)->addresses);
    EXPECT_EQ(1U, cfg.knownServers.size());
}

TEST_F(ServerRaftConsensusConfigurationTest, setStagingServers) {
    cfg.setConfiguration(1, desc(
        "prev_configuration {"
        "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
        "}"));
    cfg.setStagingServers(sdesc(
        "servers { server_id: 1, addresses: '127.0.0.1:5256' }"
        "servers { server_id: 2, addresses: '127.0.0.1:5258' }"));
    EXPECT_EQ(Configuration::State::STAGING, cfg.state);
    EXPECT_EQ(2U, cfg.newServers.servers.size());
    EXPECT_EQ(1U, cfg.newServers.servers.at(0)->serverId);
    EXPECT_EQ(2U, cfg.newServers.servers.at(1)->serverId);
    EXPECT_EQ("127.0.0.1:5256", cfg.newServers.servers.at(0)->addresses);
    EXPECT_EQ("127.0.0.1:5258", cfg.newServers.servers.at(1)->addresses);
    EXPECT_EQ(cfg.localServer, cfg.newServers.servers.at(0));

    cfg.resetStagingServers();
    EXPECT_EQ(Configuration::State::STABLE, cfg.state);
    EXPECT_EQ(0U, cfg.newServers.servers.size());
    EXPECT_EQ("127.0.0.1:5254", cfg.localServer->addresses);
    EXPECT_EQ(1U, cfg.knownServers.size());

    // TODO(ongaro): test the gc code at the end of the function
}

TEST_F(ServerRaftConsensusConfigurationTest, stagingAll) {
    // TODO(ongaro): low-priority test
}

TEST_F(ServerRaftConsensusConfigurationTest, stagingMin) {
    // TODO(ongaro): low-priority test
}

TEST_F(ServerRaftConsensusConfigurationTest, getServer) {
    EXPECT_EQ(cfg.localServer, cfg.getServer(1));
    auto s = cfg.getServer(2);
    EXPECT_EQ(2U, s->serverId);
    EXPECT_EQ(s, cfg.getServer(2));
}

class ServerRaftConsensusConfigurationManagerTest
                        : public ServerRaftConsensusConfigurationTest {
    ServerRaftConsensusConfigurationManagerTest()
        : mgr(cfg)
    {
    }
    ConfigurationManager mgr;
};

TEST_F(ServerRaftConsensusConfigurationManagerTest, add) {
    mgr.add(2, desc(d));
    EXPECT_EQ(2U, cfg.id);
    EXPECT_EQ(d, cfg.description);
    EXPECT_EQ((std::vector<uint64_t>{2}),
              Core::STLUtil::getKeys(mgr.descriptions));
    EXPECT_EQ(d, mgr.descriptions.at(2));
}

TEST_F(ServerRaftConsensusConfigurationManagerTest, truncatePrefix) {
    mgr.add(2, desc(d));
    mgr.add(3, desc(d));
    mgr.add(4, desc(d));
    mgr.truncatePrefix(3);
    EXPECT_EQ(4U, cfg.id);
    EXPECT_EQ((std::vector<uint64_t>{3, 4}),
              Core::STLUtil::getKeys(mgr.descriptions));
}

TEST_F(ServerRaftConsensusConfigurationManagerTest, truncateSuffix) {
    mgr.add(2, desc(d));
    mgr.add(3, desc(d));
    mgr.add(4, desc(d));
    mgr.truncateSuffix(3);
    EXPECT_EQ(3U, cfg.id);
    EXPECT_EQ((std::vector<uint64_t>{2, 3}),
              Core::STLUtil::getKeys(mgr.descriptions));
}

TEST_F(ServerRaftConsensusConfigurationManagerTest, setSnapshot) {
    mgr.setSnapshot(2, desc(d));
    mgr.setSnapshot(3, desc(d));
    mgr.truncatePrefix(4);
    EXPECT_EQ(3U, cfg.id);
    EXPECT_EQ((std::vector<uint64_t>{3}),
              Core::STLUtil::getKeys(mgr.descriptions));
}

TEST_F(ServerRaftConsensusConfigurationManagerTest,
                                            getLatestConfigurationAsOf) {
    std::pair<uint64_t, Protocol::Raft::Configuration> p;
    p = mgr.getLatestConfigurationAsOf(10);
    EXPECT_EQ(0U, p.first);
    mgr.add(2, desc(d));
    mgr.add(3, desc(d));
    mgr.add(4, desc(d));
    p = mgr.getLatestConfigurationAsOf(0);
    EXPECT_EQ(0U, p.first);
    p = mgr.getLatestConfigurationAsOf(2);
    EXPECT_EQ(2U, p.first);
    p = mgr.getLatestConfigurationAsOf(3);
    EXPECT_EQ(3U, p.first);
    p = mgr.getLatestConfigurationAsOf(4);
    EXPECT_EQ(4U, p.first);
    p = mgr.getLatestConfigurationAsOf(5);
    EXPECT_EQ(4U, p.first);
}

TEST_F(ServerRaftConsensusConfigurationManagerTest, restoreInvariants) {
    mgr.add(2, desc(d));
    EXPECT_EQ(2U, cfg.id);
    mgr.descriptions.clear();
    mgr.restoreInvariants();
    EXPECT_EQ(0U, cfg.id);

    mgr.add(2, desc(d));
    mgr.setSnapshot(3, desc(d));
    mgr.restoreInvariants();
    EXPECT_EQ(3U, cfg.id);
    EXPECT_EQ((std::vector<uint64_t>{2, 3}),
              Core::STLUtil::getKeys(mgr.descriptions));
}

void drainDiskQueue(RaftConsensus& consensus)
{
    assert(consensus.state == State::LEADER);
    // This is a while loop since advanceCommitIndex can append, causing
    // logSyncQueued to go true again.
    while (consensus.logSyncQueued) {
        std::unique_ptr<Log::Sync> sync = consensus.log->takeSync();
        consensus.logSyncQueued = false;
        sync->wait();
        consensus.configuration->localServer->lastSyncedIndex =
                    sync->lastIndex;
        consensus.advanceCommitIndex();
        consensus.log->syncComplete(std::move(sync));
    }
}

TEST(ServerRaftConsensusClusterClockTest, basics) {
    Clock::Mocker clockMocker;
    ClusterClock clock;
    clock.newEpoch(1000);
    EXPECT_EQ(1000U, clock.interpolate());
    Clock::mockValue += std::chrono::nanoseconds(10);
    EXPECT_EQ(1010U, clock.interpolate());
    Clock::mockValue += std::chrono::nanoseconds(10);
    EXPECT_EQ(1020U, clock.leaderStamp());
}

class ServerRaftConsensusTest : public ::testing::Test {
    ServerRaftConsensusTest()
        : storageLayout()
        , globals()
        , clockMocker()
        , consensus()
        , entry1()
        , entry2()
        , entry3()
        , entry4()
        , entry5()
    {
        globals.config.set("electionTimeoutMilliseconds", 5000);
        globals.config.set("heartbeatPeriodMilliseconds", 2500);
        globals.config.set("rpcFailureBackoffMilliseconds", 3000);
        globals.config.set("use-temporary-storage", "true");
        globals.config.set("raftDebug", "true");
        globals.config.set("stateMachineUpdaterBackoffMilliseconds", 0);

        startThreads = false;
        consensus.reset(new RaftConsensus(globals));
        consensus->SOFT_RPC_SIZE_LIMIT = 1024;
        consensus->serverId = 1;
        consensus->serverAddresses = "127.0.0.1:5254";

        entry1.set_term(1);
        entry1.set_cluster_time(0);
        entry1.set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *entry1.mutable_configuration() = desc(d);

        entry2.set_term(2);
        entry2.set_cluster_time(0);
        entry2.set_type(Protocol::Raft::EntryType::DATA);
        entry2.set_data("hello");

        entry3.set_term(3);
        entry3.set_cluster_time(0);
        entry3.set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *entry3.mutable_configuration() = desc(d2);

        entry4.set_term(4);
        entry4.set_cluster_time(0);
        entry4.set_type(Protocol::Raft::EntryType::DATA);
        entry4.set_data("goodbye");

        entry5.set_term(5);
        entry5.set_cluster_time(0);
        entry5.set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *entry5.mutable_configuration() = desc(d3);
    }
    void init() {
        consensus->log.reset(new Storage::MemoryLog());
        consensus->init();
    }
    ~ServerRaftConsensusTest()
    {
        consensus->invariants.checkAll();
        EXPECT_EQ(0U, consensus->invariants.errors);
        startThreads = true;
        consensus.reset();
    }

    Peer* getPeer(uint64_t serverId) {
        Server* server = consensus->configuration->
                            knownServers.at(serverId).get();
        return dynamic_cast<Peer*>(server);
    }

    std::shared_ptr<Peer> getPeerRef(uint64_t serverId) {
        std::shared_ptr<Server> server = consensus->configuration->
                                            knownServers.at(serverId);
        return std::dynamic_pointer_cast<Peer>(server);
    }

    Storage::Layout storageLayout;
    Globals globals;
    Clock::Mocker clockMocker;
    std::unique_ptr<RaftConsensus> consensus;
    Log::Entry entry1;
    Log::Entry entry2;
    Log::Entry entry3;
    Log::Entry entry4;
    Log::Entry entry5;
};



class ServerRaftConsensusPTest : public ServerRaftConsensusTest {
  public:
    ServerRaftConsensusPTest()
        : peerService()
        , peerServer()
        , eventLoopThread()
    {
        consensus->sessionManager.skipVerify = true;
        peerService = std::make_shared<RPC::ServiceMock>();
        peerServer.reset(new RPC::Server(globals.eventLoop,
                                     Protocol::Common::MAX_MESSAGE_LENGTH));
        RPC::Address address("127.0.0.1:5255", 0);
        address.refresh(RPC::Address::TimePoint::max());
        EXPECT_EQ("", peerServer->bind(address));
        peerServer->registerService(
                            Protocol::Common::ServiceId::RAFT_SERVICE,
                            peerService, 1);
        eventLoopThread = std::thread(&Event::Loop::runForever,
                                      &globals.eventLoop);
    }
    ~ServerRaftConsensusPTest()
    {
        globals.eventLoop.exit();
        eventLoopThread.join();
    }

    std::shared_ptr<RPC::ServiceMock> peerService;
    std::unique_ptr<RPC::Server> peerServer;
    std::thread eventLoopThread;
};

TEST_F(ServerRaftConsensusTest, init_blanklog)
{
    consensus->log.reset(new Storage::MemoryLog());
    consensus->init();
    EXPECT_EQ(0U, consensus->log->getLastLogIndex());
    EXPECT_EQ(0U, consensus->currentTerm);
    EXPECT_EQ(0U, consensus->votedFor);
    EXPECT_EQ(1U, consensus->configuration->localServer->serverId);
    EXPECT_EQ(Configuration::State::BLANK, consensus->configuration->state);
    EXPECT_EQ(0U, consensus->configuration->id);
    EXPECT_EQ(0U, consensus->commitIndex);
    EXPECT_EQ(0U, consensus->clusterClock.clusterTimeAtEpoch);
    EXPECT_EQ(Clock::mockValue, consensus->clusterClock.localTimeAtEpoch);
    EXPECT_LT(Clock::mockValue, consensus->startElectionAt);
    EXPECT_GT(Clock::mockValue + consensus->ELECTION_TIMEOUT * 2,
              consensus->startElectionAt);
}

TEST_F(ServerRaftConsensusTest, init_nonblanklog)
{
    consensus->log.reset(new Storage::MemoryLog());
    Log& log = *consensus->log.get();
    log.metadata.set_current_term(30);
    log.metadata.set_voted_for(63);
    Log::Entry entry;
    entry.set_term(1);
    entry.set_type(Protocol::Raft::EntryType::CONFIGURATION);
    *entry.mutable_configuration() = desc(d);
    entry.set_cluster_time(20);
    log.append({&entry});

    Log::Entry entry2;
    entry2.set_term(2);
    entry2.set_type(Protocol::Raft::EntryType::DATA);
    entry2.set_data("hello, world");
    entry2.set_cluster_time(30);
    log.append({&entry2});

    entry.set_term(2);
    entry.set_cluster_time(40);
    log.append({&entry}); // append configuration entry again

    consensus->init();
    EXPECT_EQ(3U, consensus->log->getLastLogIndex());
    EXPECT_EQ(30U, consensus->currentTerm);
    EXPECT_EQ(63U, consensus->votedFor);
    EXPECT_EQ(1U, consensus->configuration->localServer->serverId);
    EXPECT_EQ("127.0.0.1:5254",
              consensus->configuration->localServer->addresses);
    EXPECT_EQ(Configuration::State::STABLE, consensus->configuration->state);
    EXPECT_EQ(3U, consensus->configuration->id);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ((std::vector<uint64_t>{1, 3}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                    descriptions));
    EXPECT_EQ(40U, consensus->clusterClock.clusterTimeAtEpoch);
    EXPECT_EQ(Clock::mockValue, consensus->clusterClock.localTimeAtEpoch);
}

TEST_F(ServerRaftConsensusTest, init_withsnapshot)
{
    { // write snapshot
        RaftConsensus c1(globals);
        c1.storageLayout = std::move(consensus->storageLayout);
        c1.log.reset(new Storage::MemoryLog());
        c1.serverId = 1;
        c1.init();
        c1.currentTerm = 1;
        entry1.set_cluster_time(20);
        c1.clusterClock.newEpoch(20);
        c1.append({&entry1}); // index 1
        c1.startNewElection(); // creates noop entry
        entry1.set_term(2);
        entry1.set_cluster_time(30);
        c1.clusterClock.newEpoch(30);
        c1.append({&entry1}); // index 2
        drainDiskQueue(c1);
        EXPECT_EQ(3U, c1.commitIndex);

        std::unique_ptr<Storage::SnapshotFile::Writer> writer =
            c1.beginSnapshot(2);
        uint32_t d = 0xdeadbeef;
        writer->writeRaw(&d, sizeof(d));
        c1.snapshotDone(2, std::move(writer));
        consensus->storageLayout = std::move(c1.storageLayout);
    }

    consensus->log.reset(new Storage::MemoryLog());
    // the log should be discarded when the snapshot is read
    entry3.set_cluster_time(50);
    consensus->log->append({&entry3}); // index 1, term 3
    consensus->init();
    EXPECT_EQ(2U, consensus->lastSnapshotIndex);
    EXPECT_EQ(2U, consensus->lastSnapshotTerm);
    EXPECT_EQ(3U, consensus->log->getLogStartIndex());
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_EQ(1U, consensus->configuration->id);
    EXPECT_EQ(d, consensus->configuration->description);
    EXPECT_EQ(20U, consensus->clusterClock.clusterTimeAtEpoch);
    EXPECT_EQ(Clock::mockValue, consensus->clusterClock.localTimeAtEpoch);
}

// TODO(ongaro): low-priority test: exit

TEST_F(ServerRaftConsensusTest, bootstrapConfiguration)
{
    init();
    consensus->bootstrapConfiguration();
    EXPECT_EQ(1U, consensus->log->getLastLogIndex());
    EXPECT_EQ("term: 1 "
              "cluster_time: 0 "
              "type: CONFIGURATION "
              "configuration { "
              "  prev_configuration { "
              "    servers { server_id: 1 addresses: '127.0.0.1:5254' } "
              "  } "
              "} ",
              consensus->log->getEntry(1));
    EXPECT_DEATH(consensus->bootstrapConfiguration(),
                 "Refusing to bootstrap configuration");
}

TEST_F(ServerRaftConsensusTest, getConfiguration_notleader)
{
    init();
    Protocol::Raft::SimpleConfiguration c;
    uint64_t id;
    EXPECT_EQ(ClientResult::NOT_LEADER, consensus->getConfiguration(c, id));
}

void
setLastAckEpoch(Peer* peer)
{
    peer->lastAckEpoch = peer->consensus.currentEpoch;
}

TEST_F(ServerRaftConsensusTest, getConfiguration_retry)
{
    init();
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_EQ(2U, consensus->commitIndex);
    entry5.set_term(1);
    *entry5.mutable_configuration() = desc(d4);
    consensus->append({&entry5});
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_EQ(2U, consensus->commitIndex);
    EXPECT_EQ(3U, consensus->configuration->id);
    EXPECT_EQ(Configuration::State::TRANSITIONAL,
              consensus->configuration->state);
    consensus->stateChanged.callback = std::bind(setLastAckEpoch, getPeer(2));
    Protocol::Raft::SimpleConfiguration c;
    uint64_t id;
    EXPECT_EQ(ClientResult::RETRY, consensus->getConfiguration(c, id));
}

TEST_F(ServerRaftConsensusTest, getConfiguration_ok)
{
    init();
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    EXPECT_EQ(State::LEADER, consensus->state);
    Protocol::Raft::SimpleConfiguration c;
    uint64_t id;
    EXPECT_EQ(ClientResult::SUCCESS, consensus->getConfiguration(c, id));
    EXPECT_EQ("servers { server_id: 1, addresses: '127.0.0.1:5254' }", c);
    EXPECT_EQ(1U, id);
}

// TODO(ongaro): getLastCommitIndex: low-priority test

TEST_F(ServerRaftConsensusTest, getNextEntry)
{
    init();
    entry1.set_cluster_time(10);
    consensus->append({&entry1});
    entry2.set_cluster_time(20);
    consensus->append({&entry2});
    entry3.set_cluster_time(30);
    consensus->append({&entry3});
    entry4.set_cluster_time(40);
    consensus->append({&entry4});
    consensus->clusterClock.newEpoch(40);
    consensus->stepDown(5);
    consensus->commitIndex = 4;
    consensus->stateChanged.callback = std::bind(&RaftConsensus::exit,
                                                 consensus.get());
    RaftConsensus::Entry e1 = consensus->getNextEntry(0);
    EXPECT_EQ(1U, e1.index);
    EXPECT_EQ(RaftConsensus::Entry::SKIP, e1.type);
    EXPECT_EQ(10U, e1.clusterTime);
    RaftConsensus::Entry e2 = consensus->getNextEntry(e1.index);
    EXPECT_EQ(2U, e2.index);
    EXPECT_EQ(RaftConsensus::Entry::DATA, e2.type);
    EXPECT_EQ(20U, e2.clusterTime);
    EXPECT_EQ("hello",
              std::string(static_cast<const char*>(e2.command.getData()),
                          e2.command.getLength()));
    RaftConsensus::Entry e3 = consensus->getNextEntry(e2.index);
    EXPECT_EQ(3U, e3.index);
    EXPECT_EQ(RaftConsensus::Entry::SKIP, e3.type);
    EXPECT_EQ(30U, e3.clusterTime);
    RaftConsensus::Entry e4 = consensus->getNextEntry(e3.index);
    EXPECT_EQ(4U, e4.index);
    EXPECT_EQ(RaftConsensus::Entry::DATA, e4.type);
    EXPECT_EQ("goodbye",
              std::string(static_cast<const char*>(e4.command.getData()),
                          e4.command.getLength()));
    EXPECT_EQ(40U, e4.clusterTime);
    EXPECT_THROW(consensus->getNextEntry(e4.index),
                 Core::Util::ThreadInterruptedException);
}

TEST_F(ServerRaftConsensusTest, getNextEntry_snapshot)
{
    init();
    entry1.set_cluster_time(10);
    consensus->append({&entry1});
    consensus->clusterClock.newEpoch(10);
    consensus->startNewElection();
    entry1.set_cluster_time(20);
    consensus->append({&entry1});
    consensus->clusterClock.newEpoch(20);
    drainDiskQueue(*consensus);
    EXPECT_EQ(3U, consensus->commitIndex);

    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(2);
    uint32_t d = 0xdeadbeef;
    writer->writeRaw(&d, sizeof(d));
    consensus->snapshotDone(2, std::move(writer));
    consensus->log->truncatePrefix(2);

    // expect warning
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "ERROR"}
    });
    RaftConsensus::Entry e1 = consensus->getNextEntry(0);
    EXPECT_EQ(2U, e1.index);
    EXPECT_EQ(RaftConsensus::Entry::SNAPSHOT, e1.type);
    EXPECT_EQ(10U, e1.clusterTime);
    uint32_t x;
    EXPECT_EQ(sizeof(x),
              e1.snapshotReader->readRaw(&x, sizeof(x)));
    EXPECT_EQ(0xdeadbeef, x);

    RaftConsensus::Entry e2 = consensus->getNextEntry(2);
    EXPECT_EQ(3U, e2.index);
    EXPECT_EQ(20U, e2.clusterTime);
}

TEST_F(ServerRaftConsensusTest, getSnapshotStats)
{
    init();
    EXPECT_EQ("last_snapshot_index: 0 "
              "last_snapshot_bytes: 0 "
              "log_start_index: 1 "
              "last_log_index: 0 "
              "log_bytes: 0 "
              "is_leader: false ",
              consensus->getSnapshotStats());
    // Now try to jiggle each field and make sure it moves.
    // Can't use string comparisons since byte values are unknown.

    consensus->stepDown(1);
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    EXPECT_EQ(2U, consensus->getSnapshotStats().last_log_index());
    EXPECT_LT(10U, consensus->getSnapshotStats().log_bytes());
    EXPECT_GT(1024U, consensus->getSnapshotStats().log_bytes());
    EXPECT_TRUE(consensus->getSnapshotStats().is_leader());

    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(2);
    consensus->snapshotDone(2, std::move(writer));
    EXPECT_EQ(3U, consensus->getSnapshotStats().log_start_index());
    EXPECT_LT(10U, consensus->getSnapshotStats().last_snapshot_bytes());
    EXPECT_GT(1024U, consensus->getSnapshotStats().last_snapshot_bytes());
    EXPECT_EQ(2U, consensus->getSnapshotStats().last_snapshot_index());
}

TEST_F(ServerRaftConsensusTest, handleAppendEntries_callerStale)
{
    init();
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(8);
    request.set_prev_log_index(0);
    request.set_commit_index(0);
    consensus->stepDown(11);
    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 11 "
              "success: false "
              "last_log_index: 0"
              "server_capabilities: {}",
              response);
}

// this tests the callee stale and leaderId == 0 branches, setElectionTimer(),
// and heartbeat.
// It also makes sure the state machine capabilities are set.
TEST_F(ServerRaftConsensusTest, handleAppendEntries_newLeaderAndCommitIndex)
{
    init();
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(5);
    request.set_prev_log_index(1);
    request.set_commit_index(1);
    consensus->stepDown(8);
    consensus->append({&entry5});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    EXPECT_EQ(9U, consensus->currentTerm);
    EXPECT_EQ(0U, consensus->commitIndex);
    Clock::mockValue += milliseconds(10000);
    consensus->setSupportedStateMachineVersions(10, 20);
    consensus->handleAppendEntries(request, response);
    EXPECT_EQ(3U, consensus->leaderId);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ(0U, consensus->votedFor);
    EXPECT_EQ(10U, consensus->currentTerm);
    EXPECT_LT(Clock::mockValue, consensus->startElectionAt);
    EXPECT_GT(Clock::mockValue + consensus->ELECTION_TIMEOUT * 2,
              consensus->startElectionAt);
    EXPECT_EQ(1U, consensus->commitIndex);
    EXPECT_EQ("term: 10 "
              "success: true "
              "last_log_index: 1"
              "server_capabilities: { "
              "  min_supported_state_machine_version: 10 "
              "  max_supported_state_machine_version: 20 "
              "}",
              response);
}

TEST_F(ServerRaftConsensusTest, handleAppendEntries_rejectGap)
{
    init();
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(1);
    request.set_prev_log_index(1);
    request.set_commit_index(1);
    consensus->stepDown(10);
    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 10 "
              "success: false "
              "last_log_index: 0"
              "server_capabilities: {}",
              response);
    EXPECT_EQ(0U, consensus->commitIndex);
    EXPECT_EQ(0U, consensus->log->getLastLogIndex());
}

TEST_F(ServerRaftConsensusTest, handleAppendEntries_rejectPrevLogTerm)
{
    init();
    consensus->append({&entry1});
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(10);
    request.set_prev_log_index(1);
    request.set_commit_index(1);
    consensus->stepDown(10);
    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 10 "
              "success: false "
              "last_log_index: 1"
              "server_capabilities: {}",
              response);
    EXPECT_EQ(0U, consensus->commitIndex);
    EXPECT_EQ(1U, consensus->log->getLastLogIndex());
    EXPECT_EQ(1U, consensus->log->getEntry(1).term());
}

TEST_F(ServerRaftConsensusTest, handleAppendEntries_append)
{
    init();
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(0);
    request.set_prev_log_index(0);
    request.set_commit_index(1);
    Protocol::Raft::Entry* e1 = request.add_entries();
    e1->set_term(4);
    e1->set_type(Protocol::Raft::EntryType::CONFIGURATION);
    *e1->mutable_configuration() = desc(d3);
    e1->set_cluster_time(20);
    Protocol::Raft::Entry* e2 = request.add_entries();
    e2->set_term(5);
    e2->set_type(Protocol::Raft::EntryType::DATA);
    e2->set_cluster_time(30);
    e2->set_data("hello");
    consensus->stepDown(10);
    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 10 "
              "success: true "
              "last_log_index: 2"
              "server_capabilities: {}",
              response);
    EXPECT_EQ(1U, consensus->commitIndex);
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_EQ(1U, consensus->configuration->id);
    const Log::Entry& l1 = consensus->log->getEntry(1);
    EXPECT_EQ(4U, l1.term());
    EXPECT_EQ(Protocol::Raft::EntryType::CONFIGURATION, l1.type());
    EXPECT_EQ(d3, l1.configuration());
    const Log::Entry& l2 = consensus->log->getEntry(2);
    EXPECT_EQ(5U, l2.term());
    EXPECT_EQ(Protocol::Raft::EntryType::DATA, l2.type());
    EXPECT_EQ("hello", l2.data());
    EXPECT_EQ(30U, consensus->clusterClock.clusterTimeAtEpoch);
    EXPECT_EQ(Clock::mockValue, consensus->clusterClock.localTimeAtEpoch);
}

TEST_F(ServerRaftConsensusTest, handleAppendEntries_truncate)
{
    // Log:
    // 1,t1: config { s1 }
    // 2,t2: no op
    // 3,t2: config { s1 }
    // 4,t2: config { s1, s2 }
    // later replaced with
    // 4,t3: "bar"
    init();
    consensus->stepDown(1);
    consensus->append({&entry1});
    consensus->startNewElection();
    entry1.set_term(2);
    consensus->append({&entry1});
    drainDiskQueue(*consensus);
    entry5.set_term(2);
    entry5.set_cluster_time(80);
    consensus->append({&entry5});
    consensus->clusterClock.newEpoch(80);
    drainDiskQueue(*consensus); // shouldn't do anything
    EXPECT_EQ(3U, consensus->commitIndex);

    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(2);
    request.set_term(3);
    request.set_prev_log_term(2);
    request.set_prev_log_index(2);
    request.set_commit_index(0);
    Protocol::Raft::Entry* e1 = request.add_entries();
    e1->set_term(2);
    e1->set_type(Protocol::Raft::EntryType::CONFIGURATION);
    *e1->mutable_configuration() = entry1.configuration();
    Protocol::Raft::Entry* e2 = request.add_entries();
    e2->set_term(3);
    e2->set_type(Protocol::Raft::EntryType::DATA);
    e2->set_data("bar");
    e2->set_cluster_time(60);
    EXPECT_EQ((std::vector<uint64_t>{1, 3, 4}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                            descriptions));

    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 3 "
              "success: true "
              "last_log_index: 4"
              "server_capabilities: {}",
              response);
    EXPECT_EQ(3U, consensus->commitIndex);
    EXPECT_EQ(4U, consensus->log->getLastLogIndex());
    EXPECT_EQ(3U, consensus->configuration->id);
    const Log::Entry& l1 = consensus->log->getEntry(1);
    EXPECT_EQ(Protocol::Raft::EntryType::CONFIGURATION, l1.type());
    EXPECT_EQ(d, l1.configuration());
    const Log::Entry& l2 = consensus->log->getEntry(2);
    EXPECT_EQ(Protocol::Raft::EntryType::NOOP, l2.type());
    const Log::Entry& l3 = consensus->log->getEntry(3);
    EXPECT_EQ(d, l3.configuration());
    const Log::Entry& l4 = consensus->log->getEntry(4);
    EXPECT_EQ("bar", l4.data());
    EXPECT_EQ((std::vector<uint64_t>{1, 3}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                            descriptions));
    EXPECT_EQ(60U, consensus->clusterClock.clusterTimeAtEpoch);
    EXPECT_EQ(Clock::mockValue, consensus->clusterClock.localTimeAtEpoch);
}

TEST_F(ServerRaftConsensusTest, handleAppendEntries_duplicate)
{
    init();
    consensus->stepDown(10);
    consensus->append({&entry1});
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(0);
    request.set_prev_log_index(0);
    request.set_commit_index(0);
    Protocol::Raft::Entry* e1 = request.add_entries();
    e1->set_term(1);
    e1->set_type(Protocol::Raft::EntryType::DATA);
    e1->set_data("hello");
    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 10 "
              "success: true "
              "last_log_index: 1"
              "server_capabilities: {}",
              response);
    EXPECT_EQ(1U, consensus->log->getLastLogIndex());
    const Log::Entry& l1 = consensus->log->getEntry(1);
    EXPECT_EQ(Protocol::Raft::EntryType::CONFIGURATION, l1.type());
    EXPECT_EQ(d, l1.configuration());
    EXPECT_EQ("", l1.data());
}

// entry is part of existing snapshot
TEST_F(ServerRaftConsensusTest, handleAppendEntries_appendSnapshotOk)
{
    init();
    consensus->stepDown(10);
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_prev_log_term(1);
    request.set_prev_log_index(1);
    request.set_commit_index(0);
    Protocol::Raft::Entry* e1 = request.add_entries();
    e1->set_term(1);
    e1->set_type(Protocol::Raft::EntryType::DATA);
    e1->set_data("hello");

    consensus->log->truncatePrefix(5);
    consensus->append({&entry1});
    consensus->lastSnapshotIndex = 5;
    consensus->commitIndex = 5;

    consensus->handleAppendEntries(request, response);
    EXPECT_EQ("term: 10 "
              "success: true "
              "last_log_index: 5"
              "server_capabilities: {}",
              response);
    EXPECT_EQ(5U, consensus->log->getLastLogIndex());
}

std::string
readEntireFileAsString(const Storage::FilesystemUtil::File& parentDir,
                       const std::string& name)
{
    Storage::FilesystemUtil::FileContents f(
        Storage::FilesystemUtil::openFile(parentDir, name, O_RDONLY));
    return std::string(f.get<char>(0, f.getFileLength()),
                       f.getFileLength());
}

TEST_F(ServerRaftConsensusTest, handleInstallSnapshot_callerStale)
{
    init();
    Protocol::Raft::InstallSnapshot::Request request;
    Protocol::Raft::InstallSnapshot::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_last_snapshot_index(1);
    request.set_byte_offset(0);
    request.set_data("hello");
    request.set_done(false);
    consensus->stepDown(11);
    consensus->handleInstallSnapshot(request, response);
    EXPECT_EQ("term: 11 ", response);
}

// this tests the callee stale and leaderId == 0 branches and
// setElectionTimer()
TEST_F(ServerRaftConsensusTest, handleSnapshotChunk_newLeader)
{
    init();
    Protocol::Raft::InstallSnapshot::Request request;
    Protocol::Raft::InstallSnapshot::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_last_snapshot_index(1);
    request.set_byte_offset(0);
    request.set_data("hello");
    request.set_done(false);
    consensus->stepDown(8);
    consensus->append({&entry5});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    EXPECT_EQ(9U, consensus->currentTerm);
    Clock::mockValue += milliseconds(10000);
    consensus->handleInstallSnapshot(request, response);
    EXPECT_EQ(3U, consensus->leaderId);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ(0U, consensus->votedFor);
    EXPECT_EQ(10U, consensus->currentTerm);
    EXPECT_LT(Clock::mockValue, consensus->startElectionAt);
    EXPECT_GT(Clock::mockValue + consensus->ELECTION_TIMEOUT * 2,
              consensus->startElectionAt);
    EXPECT_EQ("term: 10 "
              "bytes_stored: 5", response);
    consensus->snapshotWriter->discard();
}

TEST_F(ServerRaftConsensusTest, handleInstallSnapshot)
{
    init();
    consensus->stepDown(10);
    consensus->append({&entry1});
    consensus->commitIndex = 1;

    // Take a snapshot, saving it directly instead of calling snapshotDone().
    // This way, the consensus module does not know about the snapshot file.
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(1);
    writer->save();
    std::string snapshotContents =
        readEntireFileAsString(consensus->storageLayout.snapshotDir,
                               "snapshot");

    Protocol::Raft::InstallSnapshot::Request request;
    Protocol::Raft::InstallSnapshot::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_last_snapshot_index(1);
    request.set_byte_offset(0);
    request.set_data(snapshotContents);
    request.set_done(false);

    // useful data, but not done yet
    consensus->handleInstallSnapshot(request, response);
    EXPECT_EQ("term: 10 "
              "bytes_stored: 37", response);
    EXPECT_EQ(0U, consensus->lastSnapshotIndex);
    EXPECT_TRUE(bool(consensus->snapshotWriter));

    // stale packet: expect warning
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "ERROR"}
    });
    consensus->handleInstallSnapshot(request, response);
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "WARNING"}
    });
    EXPECT_EQ("term: 10 "
              "bytes_stored: 37", response);
    EXPECT_EQ(0U, consensus->lastSnapshotIndex);
    EXPECT_TRUE(bool(consensus->snapshotWriter));

    // done now
    request.set_byte_offset(snapshotContents.size());
    request.set_data("hello world!");
    request.set_done(true);
    consensus->handleInstallSnapshot(request, response);
    EXPECT_EQ("term: 10 "
              "bytes_stored: 49", response);
    EXPECT_EQ(1U, consensus->lastSnapshotIndex);
    EXPECT_FALSE(bool(consensus->snapshotWriter));
    char helloWorld[13];
    EXPECT_EQ(sizeof(helloWorld) - 1,
              consensus->snapshotReader->readRaw(helloWorld,
                                                 sizeof(helloWorld) - 1));
    helloWorld[sizeof(helloWorld) - 1] = '\0';
    EXPECT_STREQ("hello world!", helloWorld);

    // TODO(ongaro): Test that the configuration is update accordingly
}

TEST_F(ServerRaftConsensusTest, handleInstallSnapshot_byteOffsetHigh)
{
    init();
    consensus->stepDown(10);
    consensus->append({&entry1});
    consensus->commitIndex = 1;

    // Take a snapshot, saving it directly instead of calling snapshotDone().
    // This way, the consensus module does not know about the snapshot file.
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(1);
    writer->save();
    std::string snapshotContents =
        readEntireFileAsString(consensus->storageLayout.snapshotDir,
                               "snapshot");

    Protocol::Raft::InstallSnapshot::Request request;
    Protocol::Raft::InstallSnapshot::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_last_snapshot_index(1);
    request.set_byte_offset(1);
    request.set_data(snapshotContents);
    request.set_done(false);

    // expect warnings
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "ERROR"}
    });

    // byte offset higher than offset written,
    // version 2 behavior
    request.set_version(2);
    consensus->handleInstallSnapshot(request, response);
    EXPECT_EQ("term: 10 "
              "bytes_stored: 0",
              response);
    request.clear_version();

    // byte offset higher than offset written,
    // version 1 compatibility
    consensus->handleInstallSnapshot(request, response);
    EXPECT_EQ("term: 11 "
              "bytes_stored: 0",
              response);
    EXPECT_EQ(11U, consensus->currentTerm);
}

TEST_F(ServerRaftConsensusTest, handleRequestVote)
{
    init();
    Protocol::Raft::RequestVote::Request request;
    Protocol::Raft::RequestVote::Response response;
    request.set_server_id(3);
    request.set_term(12);
    request.set_last_log_term(1);
    request.set_last_log_index(2);

    // as leader, log is ok: don't update term or grant vote
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    EXPECT_EQ(State::LEADER, consensus->state);
    consensus->handleRequestVote(request, response);
    EXPECT_EQ("term: 1 "
              "granted: false "
              "log_ok: true",
              response);
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_EQ(1U, consensus->currentTerm);
    EXPECT_EQ(1U, consensus->votedFor);

    // as candidate, log is not ok
    consensus->stepDown(5);
    consensus->append({&entry5});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    TimePoint oldStartElectionAt = consensus->startElectionAt;
    Clock::mockValue += milliseconds(2);
    consensus->handleRequestVote(request, response);
    EXPECT_EQ("term: 12 "
              "granted: false "
              "log_ok: false",
              response);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    // check that the election timer was not reset
    EXPECT_EQ(oldStartElectionAt, consensus->startElectionAt);
    EXPECT_EQ(0U, consensus->votedFor);

    // as candidate, log is ok
    request.set_last_log_term(9);
    consensus->handleRequestVote(request, response);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ("term: 12 "
              "granted: true "
              "log_ok: true",
              response);
    EXPECT_EQ(3U, consensus->votedFor);
}

TEST_F(ServerRaftConsensusTest, handleRequestVote_termStale)
{
    init();
    Protocol::Raft::RequestVote::Request request;
    Protocol::Raft::RequestVote::Response response;
    request.set_server_id(3);
    request.set_term(10);
    request.set_last_log_term(1);
    request.set_last_log_index(1);
    consensus->stepDown(11);
    consensus->handleRequestVote(request, response);
    EXPECT_EQ("term: 11 "
              "granted: false "
              "log_ok: true",
              response);
    Clock::mockValue += milliseconds(100000);
    // don't hand out vote, don't reset follower timer
    EXPECT_EQ(0U, consensus->votedFor);
    EXPECT_GT(Clock::mockValue, consensus->startElectionAt);
}

// TODO(ongardie): low-priority test: replicate

TEST_F(ServerRaftConsensusTest, setConfiguration_notLeader)
{
    init();
    Protocol::Client::SetConfiguration::Request request;
    Protocol::Client::SetConfiguration::Response response;
    request.set_old_id(1);
    EXPECT_EQ(ClientResult::NOT_LEADER,
              consensus->setConfiguration(request, response));
}

TEST_F(ServerRaftConsensusTest, setConfiguration_changed)
{
    init();
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);

    Protocol::Client::SetConfiguration::Request request;
    Protocol::Client::SetConfiguration::Response response;
    request.set_old_id(0);
    EXPECT_EQ(ClientResult::FAIL,
              consensus->setConfiguration(request, response));
    EXPECT_TRUE(response.has_configuration_changed());

    consensus->configuration->setStagingServers(sdesc(""));
    consensus->stateChanged.notify_all();
    EXPECT_EQ(Configuration::State::STAGING, consensus->configuration->state);

    response.Clear();
    EXPECT_EQ(ClientResult::FAIL,
              consensus->setConfiguration(request, response));
    EXPECT_TRUE(response.has_configuration_changed());
}

void
setConfigurationHelper(RaftConsensus* consensus)
{
    TimePoint waitUntil(consensus->stateChanged.lastWaitUntil);
    EXPECT_EQ(Clock::mockValue + consensus->ELECTION_TIMEOUT,
              waitUntil);
    Clock::mockValue += consensus->ELECTION_TIMEOUT;
}

TEST_F(ServerRaftConsensusTest, setConfiguration_catchupFail)
{
    init();
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    consensus->stateChanged.callback = std::bind(setConfigurationHelper,
                                                 consensus.get());

    Protocol::Client::SetConfiguration::Request request;
    Protocol::Client::SetConfiguration::Response response;
    request = Core::ProtoBuf::fromString<
        Protocol::Client::SetConfiguration::Request>(
        "old_id: 1 "
        "new_servers { server_id: 2, addresses: '127.0.0.1:5255' }");

    EXPECT_EQ(ClientResult::FAIL,
              consensus->setConfiguration(request, response));
    EXPECT_EQ("configuration_bad { "
                  "bad_servers { "
                      "server_id: 2 "
                      "addresses: '127.0.0.1:5255' "
                  "}"
              "}",
              response);
}

void
setConfigurationHelper2(RaftConsensus* consensus)
{
    Server* server = consensus->configuration->knownServers.at(2).get();
    Peer* peer = dynamic_cast<Peer*>(server);
    peer->isCaughtUp_ = true;
    consensus->stateChanged.callback = std::bind(&RaftConsensus::stepDown,
                                                 consensus, 10);
}

TEST_F(ServerRaftConsensusTest, setConfiguration_replicateFail)
{
    init();
    consensus->append({&entry1});
    consensus->stepDown(1);
    consensus->startNewElection();
    consensus->stateChanged.callback = std::bind(setConfigurationHelper2,
                                                 consensus.get());
    Protocol::Client::SetConfiguration::Request request;
    Protocol::Client::SetConfiguration::Response response;
    request = Core::ProtoBuf::fromString<
        Protocol::Client::SetConfiguration::Request>(
        "old_id: 1 "
        "new_servers { server_id: 2, addresses: '127.0.0.1:5255' }");

    EXPECT_EQ(ClientResult::NOT_LEADER,
              consensus->setConfiguration(request, response));

    // 1: entry1, 2: no-op, 3: transitional
    EXPECT_EQ(3U, consensus->log->getLastLogIndex());
    const Log::Entry& l2 = consensus->log->getEntry(3);
    EXPECT_EQ(Protocol::Raft::EntryType::CONFIGURATION, l2.type());
    EXPECT_EQ("prev_configuration {"
                  "servers { server_id: 1, addresses: '127.0.0.1:5254' }"
              "}"
              "next_configuration {"
                  "servers { server_id: 2, addresses: '127.0.0.1:5255' }"
              "}",
              l2.configuration());
}

TEST_F(ServerRaftConsensusTest, setConfiguration_replicateOkJustUs)
{
    init();
    consensus->append({&entry1});
    consensus->stepDown(1);
    consensus->startNewElection();
    consensus->leaderDiskThread =
        std::thread(&RaftConsensus::leaderDiskThreadMain, consensus.get());
    Protocol::Client::SetConfiguration::Request request;
    Protocol::Client::SetConfiguration::Response response;
    request = Core::ProtoBuf::fromString<
        Protocol::Client::SetConfiguration::Request>(
        "old_id: 1 "
        "new_servers { server_id: 1, addresses: '127.0.0.1:5255' }");

    EXPECT_EQ(ClientResult::SUCCESS,
              consensus->setConfiguration(request, response));

    // 1: entry1, 2: no-op, 3: transitional, 4: new config
    EXPECT_EQ(4U, consensus->log->getLastLogIndex());
    const Log::Entry& l3 = consensus->log->getEntry(4);
    EXPECT_EQ(Protocol::Raft::EntryType::CONFIGURATION, l3.type());
    EXPECT_EQ("prev_configuration {"
                  "servers { server_id: 1, addresses: '127.0.0.1:5255' }"
              "}",
              l3.configuration());
}

// used in setConfiguration_replicateOkNontrivial
class SetConfigurationHelper3 {
    explicit SetConfigurationHelper3(RaftConsensus* consensus)
        : consensus(consensus)
        , iter(1)
    {
    }
    void operator()() {
        Server* server = consensus->configuration->knownServers.at(2).get();
        Peer* peer = dynamic_cast<Peer*>(server);
        if (iter == 1) {
            peer->isCaughtUp_ = true;
        } else if (iter == 2) { // no-op entry
            drainDiskQueue(*consensus);
            peer->matchIndex = 2;
            consensus->advanceCommitIndex();
        } else if (iter == 3) { // transitional entry
            drainDiskQueue(*consensus);
            peer->matchIndex = 3;
            consensus->advanceCommitIndex();
        } else if (iter == 4) { // new configuration entry
            drainDiskQueue(*consensus);
            peer->matchIndex = 4;
            consensus->advanceCommitIndex();
        } else {
            FAIL();
        }
        ++iter;
    }
    RaftConsensus* consensus;
    uint64_t iter;
};

TEST_F(ServerRaftConsensusTest, setConfiguration_replicateOkNontrivial)
{
    // Log:
    // 1,t1: cfg { server 1 }
    // 2,t2: no op
    // 3,t2: cfg { server 1 } to { server 2 }
    // 4,t2: cfg { server 2 }
    init();
    consensus->append({&entry1});
    consensus->stepDown(1);
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    consensus->stateChanged.callback =
        SetConfigurationHelper3(consensus.get());
    Protocol::Client::SetConfiguration::Request request;
    Protocol::Client::SetConfiguration::Response response;
    request = Core::ProtoBuf::fromString<
        Protocol::Client::SetConfiguration::Request>(
        "old_id: 1 "
        "new_servers { server_id: 2, addresses: '127.0.0.1:5255' }");
    EXPECT_EQ(ClientResult::SUCCESS,
              consensus->setConfiguration(request, response));
    EXPECT_EQ(4U, consensus->log->getLastLogIndex());
}

TEST_F(ServerRaftConsensusTest, setSupportedStateMachineVersions)
{
    init();
    auto& s = *consensus->configuration->localServer;
    EXPECT_FALSE(s.haveStateMachineSupportedVersions);
    consensus->stateChanged.notificationCount = 0;

    consensus->setSupportedStateMachineVersions(10, 20);
    EXPECT_TRUE(s.haveStateMachineSupportedVersions);
    EXPECT_EQ(10U, s.minStateMachineVersion);
    EXPECT_EQ(20U, s.maxStateMachineVersion);
    EXPECT_EQ(1U, consensus->stateChanged.notificationCount);

    consensus->setSupportedStateMachineVersions(10, 20);
    EXPECT_TRUE(s.haveStateMachineSupportedVersions);
    EXPECT_EQ(10U, s.minStateMachineVersion);
    EXPECT_EQ(20U, s.maxStateMachineVersion);
    EXPECT_EQ(1U, consensus->stateChanged.notificationCount);

    consensus->setSupportedStateMachineVersions(10, 21);
    EXPECT_TRUE(s.haveStateMachineSupportedVersions);
    EXPECT_EQ(10U, s.minStateMachineVersion);
    EXPECT_EQ(21U, s.maxStateMachineVersion);
    EXPECT_EQ(2U, consensus->stateChanged.notificationCount);

    consensus->setSupportedStateMachineVersions(11, 21);
    EXPECT_TRUE(s.haveStateMachineSupportedVersions);
    EXPECT_EQ(11U, s.minStateMachineVersion);
    EXPECT_EQ(21U, s.maxStateMachineVersion);
    EXPECT_EQ(3U, consensus->stateChanged.notificationCount);
}

TEST_F(ServerRaftConsensusTest, beginSnapshot)
{
    // Log:
    // 1,t1: cfg { server 1 }
    // 2,t2: no op
    // 3,t2: entry2
    // 4,t2: entry5

    init();

    // satisfy commitIndex >= lastSnapshotIndex invariant
    consensus->currentTerm = 1;
    entry1.set_cluster_time(10);
    consensus->clusterClock.newEpoch(10);
    consensus->append({&entry1});
    consensus->startNewElection();
    entry2.set_cluster_time(30);
    consensus->clusterClock.newEpoch(30);
    consensus->append({&entry2});
    drainDiskQueue(*consensus);
    entry5.set_term(2);
    entry5.set_cluster_time(40);
    consensus->clusterClock.newEpoch(40);
    consensus->append({&entry5});
    drainDiskQueue(*consensus);
    EXPECT_EQ(3U, consensus->commitIndex);

    // call beginSnapshot
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(3);
    uint32_t d = 0xdeadbeef;
    writer->writeRaw(&d, sizeof(d));
    writer->save();

    // make sure it had the right side-effects
    EXPECT_EQ(0U, consensus->lastSnapshotIndex);
    consensus->configurationManager->descriptions.erase(1);
    EXPECT_EQ((std::vector<uint64_t>{4}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                                descriptions));
    consensus->readSnapshot();
    EXPECT_EQ(3U, consensus->lastSnapshotIndex);
    EXPECT_EQ(2U, consensus->lastSnapshotTerm);
    EXPECT_EQ(30U, consensus->lastSnapshotClusterTime);
    uint32_t x = 0;
    EXPECT_EQ(sizeof(x),
              consensus->snapshotReader->readRaw(&x, sizeof(x)));
    EXPECT_EQ(0xdeadbeef, x);
    EXPECT_EQ((std::vector<uint64_t>{1, 4}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                                descriptions));
}

TEST_F(ServerRaftConsensusTest, snapshotDone)
{
    init();

    // satisfy commitIndex >= lastSnapshotIndex invariant
    consensus->currentTerm = 1;
    entry1.set_cluster_time(60);
    consensus->clusterClock.newEpoch(60);
    consensus->append({&entry1});
    consensus->startNewElection();
    entry2.set_cluster_time(80);
    consensus->clusterClock.newEpoch(80);
    consensus->append({&entry2});
    drainDiskQueue(*consensus);
    EXPECT_EQ(3U, consensus->commitIndex);

    // this one will get discarded
    std::unique_ptr<Storage::SnapshotFile::Writer> discardWriter =
        consensus->beginSnapshot(2);
    // this one will get saved
    std::unique_ptr<Storage::SnapshotFile::Writer> saveWriter =
        consensus->beginSnapshot(3);

    consensus->snapshotDone(3, std::move(saveWriter));
    EXPECT_EQ(3U, consensus->lastSnapshotIndex);
    EXPECT_EQ(2U, consensus->lastSnapshotTerm);
    EXPECT_EQ(80U, consensus->lastSnapshotClusterTime);
    // Don't really know exactly how big the snapshot will be in bytes, but
    // between 10 and 1K seems reasonable.
    EXPECT_LT(10U, consensus->lastSnapshotBytes);
    EXPECT_GT(1024U, consensus->lastSnapshotBytes);
    EXPECT_EQ(1U, consensus->configuration->id);
    EXPECT_EQ(4U, consensus->log->getLogStartIndex());

    consensus->snapshotDone(2, std::move(discardWriter));
    EXPECT_EQ(3U, consensus->lastSnapshotIndex);
    EXPECT_EQ(2U, consensus->lastSnapshotTerm);
    EXPECT_EQ(80U, consensus->lastSnapshotClusterTime);
    EXPECT_EQ(1U, consensus->configuration->id);
}

class StateMachineUpdaterThreadMainHelper {
    StateMachineUpdaterThreadMainHelper(
            RaftConsensus& consensus,
            Peer& peer)
        : consensus(consensus)
        , iter(1)
        , peer(peer)
    {
    }
    void operator()() {
        NOTICE("iter: %lu", iter);
        // not leader: go to sleep
        if (iter == 1) {
            consensus.startNewElection();
            consensus.becomeLeader();
        // leader but missing info from peer: go to sleep
        } else if (iter == 2) {
            EXPECT_EQ(RaftConsensus::State::LEADER, consensus.state);
            peer.haveStateMachineSupportedVersions = true;
            peer.minStateMachineVersion = 4;
            peer.maxStateMachineVersion = 9;
            LogCabin::Core::Debug::setLogPolicy({
                {"", "SILENT"}
            });
        // leader and all info but servers don't overlap: go to sleep
        } else if (iter == 3) {
            LogCabin::Core::Debug::setLogPolicy({
                {"", "WARNING"}
            });
            consensus.setSupportedStateMachineVersions(1, 4);
            EXPECT_EQ(2U, consensus.log->getLastLogIndex());
        // leader and all info and servers overlap on new version: append
        // entry. the next wait is in replicateEntry
        } else if (iter == 4) {
            std::string commandString = consensus.log->getEntry(3).data();
            Core::Buffer commandBuffer(
                const_cast<char*>(commandString.data()),
                commandString.length(),
                NULL);
            Protocol::Client::StateMachineCommand::Request command;
            EXPECT_TRUE(Core::ProtoBuf::parse(commandBuffer, command));
            EXPECT_EQ("advance_version { "
                      "  requested_version: 4 "
                      "}", command);
            consensus.stepDown(7);
            LogCabin::Core::Debug::setLogPolicy({
                {"", "ERROR"}
            });
        // replicateEntry failed because lost leadership: issue warning
        } else if (iter == 5) {
            LogCabin::Core::Debug::setLogPolicy({
                {"", "WARNING"}
            });
            EXPECT_EQ(3U, consensus.log->getLastLogIndex());
            consensus.startNewElection();
            consensus.becomeLeader();
        // leader and all info and servers overlap on new version: append
        // another entry for v4. the next wait is in replicateEntry
        } else if (iter == 6) {
            std::string commandString = consensus.log->getEntry(5).data();
            Core::Buffer commandBuffer(
                const_cast<char*>(commandString.data()),
                commandString.length(),
                NULL);
            Protocol::Client::StateMachineCommand::Request command;
            EXPECT_TRUE(Core::ProtoBuf::parse(commandBuffer, command));
            EXPECT_EQ("advance_version { "
                      "  requested_version: 4 "
                      "}", command);
            peer.matchIndex = 5;
            consensus.commitIndex = 5;
            consensus.stateChanged.notify_all(); // to satisfy RaftInvariants
        // leader and all info and servers overlap on current version: go to
        // sleep
        } else if (iter == 7) {
            EXPECT_EQ(5U, consensus.log->getLastLogIndex());
            consensus.exit();
        }
        ++iter;
    }
    RaftConsensus& consensus;
    uint64_t iter;
    Peer& peer;
};

TEST_F(ServerRaftConsensusTest, stateMachineUpdaterThreadMain)
{
    // Log:
    // 1,t5: cfg { server 1:5254, server 2:5255 }
    // After iter1:
    // 2,t6: no op
    // After iter3:
    // 3,t6: data { advance state machine to version 4 }
    // After iter5:
    // 4,t7: no op
    // 5,t7: data { advance state machine to version 4 }
    init();
    consensus->stepDown(5);
    *entry5.mutable_configuration() = desc(d3);
    consensus->append({&entry5});
    consensus->setSupportedStateMachineVersions(1, 3);
    std::shared_ptr<Peer> peer = getPeerRef(2);
    StateMachineUpdaterThreadMainHelper helper(*consensus, *peer);
    consensus->stateChanged.callback = std::ref(helper);
    consensus->stateMachineUpdaterThreadMain();
    EXPECT_EQ(8U, helper.iter);
}

class BumpTermSync : public Log::Sync {
  protected:
    explicit BumpTermSync(RaftConsensus& consensus)
        : Log::Sync(20)
        , consensus(consensus)
        , first(true)
    {
    }
    void wait() {
        if (first) {
            first = false;
            // clear leaderDiskThreadWorking or stepDown will block forever
            consensus.leaderDiskThreadWorking = false;
            consensus.stepDown(consensus.currentTerm + 1);
        }
    }
    RaftConsensus& consensus;
    bool first;
};

class DiskThreadMainHelper {
    explicit DiskThreadMainHelper(RaftConsensus& consensus)
        : consensus(consensus)
        , iter(1)
    {
    }
    void operator()() {
        Storage::MemoryLog* log =
            dynamic_cast<Storage::MemoryLog*>(consensus.log.get());
        EXPECT_FALSE(consensus.leaderDiskThreadWorking);
        if (iter == 1) {
            EXPECT_FALSE(consensus.logSyncQueued);
            EXPECT_EQ(2U,
                      consensus.configuration->localServer->lastSyncedIndex);
            EXPECT_EQ(2U, consensus.commitIndex);
        } else if (iter == 2) {
            EXPECT_FALSE(consensus.logSyncQueued);
            log->currentSync->completed = true;
            log->currentSync.reset(new BumpTermSync(consensus));
            consensus.logSyncQueued = true;
        } else if (iter == 3) {
            EXPECT_FALSE(consensus.logSyncQueued);
            EXPECT_EQ(2U,
                      consensus.configuration->localServer->lastSyncedIndex);
            EXPECT_EQ(2U, consensus.commitIndex);
            log->currentSync->lastIndex = 4;
            consensus.logSyncQueued = true;
        } else if (iter == 4) {
            EXPECT_TRUE(consensus.logSyncQueued);
            consensus.exit();
        }
        ++iter;
    }
    RaftConsensus& consensus;
    uint64_t iter;
};

TEST_F(ServerRaftConsensusTest, leaderDiskThreadMain)
{
    // iter 1: leader with sync to do
    // iter 2: leader with nothing to do
    // iter 3: leader with sync to do, different term
    // iter 4: not leader, sync to do
    // iter 5: exit

    // Log:
    // 1,t1: cfg { server 1:5254 }
    // 2,t6: no op

    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_TRUE(consensus->logSyncQueued);
    DiskThreadMainHelper helper(*consensus);
    consensus->stateChanged.callback = std::ref(helper);
    consensus->leaderDiskThreadMain();
    EXPECT_EQ(5U, helper.iter);
}

class CandidacyThreadMainHelper {
    explicit CandidacyThreadMainHelper(RaftConsensus& consensus)
        : consensus(consensus)
        , iter(1)
    {
    }
    void operator()() {
        if (iter == 1) {
            EXPECT_EQ(State::FOLLOWER, consensus.state);
            Clock::mockValue = consensus.startElectionAt + milliseconds(1);
        } else {
            EXPECT_EQ(State::CANDIDATE, consensus.state);
            consensus.exit();
        }
        ++iter;
    }
    RaftConsensus& consensus;
    int iter;
};

// The first time through the while loop, we don't want to start a new election
// and want to wait on the condition variable. The second time through, we want
// to start a new election. Then we want to exit.
TEST_F(ServerRaftConsensusTest, timerThreadMain)
{
    init();
    Clock::mockValue = consensus->startElectionAt - milliseconds(1);
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->append({&entry5});
    consensus->stateChanged.callback = CandidacyThreadMainHelper(*consensus);
    consensus->timerThreadMain();
}

// used in peerThreadMain test
class FollowerThreadMainHelper {
    explicit FollowerThreadMainHelper(RaftConsensus& consensus, Peer& peer)
        : consensus(consensus)
        , peer(peer)
        , iter(1)
    {
    }
    void operator()() {
        TimePoint waitUntil(consensus.stateChanged.lastWaitUntil);

        if (iter == 1) {
            // expect to block forever as a follower
            EXPECT_EQ(TimePoint::max(), waitUntil);
            // set the peer's backoff to prepare for next iteration
            peer.backoffUntil = Clock::mockValue + milliseconds(1);
        } else if (iter == 2) {
            // still a follower so nothing to do, but this time we have to
            // block until backoff is over
            EXPECT_EQ(Clock::mockValue + milliseconds(1), waitUntil);
            Clock::mockValue += milliseconds(2);
            // move to candidacy
            consensus.startNewElection();
        } else if (iter == 3) {
            // we should have just requested peer's vote, so expect to return
            // immediately
            EXPECT_EQ(TimePoint::min(), waitUntil);
        } else if (iter == 4) {
            // the vote was granted, so there's nothing left to do for this
            // peer as a candidate, sleep forever
            EXPECT_EQ(TimePoint::max(), waitUntil);
            // move to leader state
            consensus.becomeLeader();
            // This test was written assuming peer's nextIndex starts one past
            // the end of the log. The code was since changed to point
            // nextIndex to the nop entry.
            EXPECT_EQ(2U, peer.nextIndex);
            peer.nextIndex = 3;
        } else if (iter == 5) {
            // we should have just sent a heartbeat, so expect to return
            // immediately
            EXPECT_EQ(TimePoint::min(), waitUntil);
        } else if (iter == 6) {
            // expect to block until the next heartbeat
            EXPECT_EQ(peer.nextHeartbeatTime, waitUntil);
            Clock::mockValue = peer.nextHeartbeatTime + milliseconds(1);
        } else if (iter == 7) {
            // we should have just sent a heartbeat, so expect to return
            // immediately
            EXPECT_EQ(TimePoint::min(), waitUntil);
        } else if (iter == 8) {
            // expect to block until the next heartbeat
            EXPECT_EQ(peer.nextHeartbeatTime, waitUntil);
            // exit
            consensus.exit();
            EXPECT_TRUE(peer.exiting);
        } else {
            FAIL() << iter;
        }
        ++iter;
    }
    RaftConsensus& consensus;
    Peer& peer;
    int iter;
};

TEST_F(ServerRaftConsensusPTest, peerThreadMain)
{
    // Log:
    // 1,t5: cfg { server 1,2,3,4,5 }
    // 2,t6: no-op
    init();
    consensus->stepDown(5);
    *entry5.mutable_configuration() = desc(
        "prev_configuration {"
        "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
        "    servers { server_id: 2, addresses: '127.0.0.1:5255' }"
        "    servers { server_id: 3, addresses: '127.0.0.1:5255' }"
        "    servers { server_id: 4, addresses: '127.0.0.1:5255' }"
        "    servers { server_id: 5, addresses: '127.0.0.1:5255' }"
        "}");
    consensus->append({&entry5});
    std::shared_ptr<Peer> peer = getPeerRef(2);
    consensus->stateChanged.callback = FollowerThreadMainHelper(*consensus,
                                                                *peer);
    ++consensus->numPeerThreads;

    // first requestVote RPC succeeds
    Protocol::Raft::RequestVote::Request vrequest;
    vrequest.set_server_id(1);
    vrequest.set_term(6);
    vrequest.set_last_log_term(5);
    vrequest.set_last_log_index(1);
    Protocol::Raft::RequestVote::Response vresponse;
    vresponse.set_term(5);
    vresponse.set_granted(true);
    peerService->reply(Protocol::Raft::OpCode::REQUEST_VOTE,
                       vrequest, vresponse);

    // first appendEntries sends heartbeat (accept it)
    Protocol::Raft::AppendEntries::Request arequest;
    arequest.set_server_id(1);
    arequest.set_term(6);
    arequest.set_prev_log_term(6);
    arequest.set_prev_log_index(2);
    arequest.set_commit_index(0);
    Protocol::Raft::AppendEntries::Response aresponse;
    aresponse.set_term(6);
    aresponse.set_success(true);
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       arequest, aresponse);

    // second appendEntries sends heartbeat
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       arequest, aresponse);

    consensus->peerThreadMain(peer);
}

class StepDownThreadMainHelper {
    explicit StepDownThreadMainHelper(RaftConsensus& consensus)
        : consensus(consensus)
        , iter(1)
    {
    }
    void operator()() {
        if (iter == 1) {
            consensus.startNewElection();
        } else {
            consensus.exit();
        }
        ++iter;
    }
    RaftConsensus& consensus;
    int iter;
};

TEST_F(ServerRaftConsensusTest, stepDownThreadMain_oneServerNoInfiniteLoop)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->stateChanged.callback = StepDownThreadMainHelper(*consensus);
    consensus->stepDownThreadMain();
    EXPECT_EQ(State::LEADER, consensus->state);
}

class StepDownThreadMainHelper2 {
    explicit StepDownThreadMainHelper2(RaftConsensus& consensus,
                                       Peer& peer)
        : consensus(consensus)
        , peer(peer)
        , iter(1)
    {
    }
    void operator()() {
        if (iter == 1) {
            EXPECT_EQ(1U, consensus.currentEpoch);
            consensus.stepDown(consensus.currentTerm + 1);
            consensus.startNewElection();
            consensus.becomeLeader();
        } else if (iter == 2) {
            EXPECT_EQ(2U, consensus.currentEpoch);
            peer.lastAckEpoch = 2;
        } else if (iter == 3) {
            EXPECT_EQ(3U, consensus.currentEpoch);
            Clock::mockValue += consensus.ELECTION_TIMEOUT;
        } else if (iter == 4) {
            EXPECT_EQ(3U, consensus.currentEpoch);
            consensus.exit();
        } else {
            FAIL();
        }
        ++iter;
    }
    RaftConsensus& consensus;
    Peer& peer;
    int iter;
};


TEST_F(ServerRaftConsensusTest, stepDownThreadMain_twoServers)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry5});
    consensus->startNewElection();
    consensus->becomeLeader();
    consensus->currentEpoch = 0;
    consensus->stateChanged.callback = StepDownThreadMainHelper2(*consensus,
                                                                 *getPeer(2));
    consensus->stepDownThreadMain();
}

TEST_F(ServerRaftConsensusTest, advanceCommitIndex_noAdvanceMissingQuorum)
{
    init();
    consensus->append({&entry1});
    consensus->append({&entry5});
    consensus->stepDown(5);
    consensus->startNewElection();
    consensus->becomeLeader();
    consensus->advanceCommitIndex();
    drainDiskQueue(*consensus);
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_EQ(0U, consensus->commitIndex);
}

TEST_F(ServerRaftConsensusTest,
       advanceCommitIndex_noAdvanceNoEntryFromCurrentTerm)
{
    init();
    consensus->append({&entry1});
    consensus->append({&entry5});
    consensus->stepDown(5);
    consensus->startNewElection();
    consensus->becomeLeader();
    drainDiskQueue(*consensus);
    getPeer(2)->matchIndex = 2;
    consensus->advanceCommitIndex();
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_EQ(0U, consensus->commitIndex);
    getPeer(2)->matchIndex = 3;
    consensus->advanceCommitIndex();
    EXPECT_EQ(3U, consensus->commitIndex);
}

TEST_F(ServerRaftConsensusTest, advanceCommitIndex_commitCfgWithoutSelf)
{
    // Log:
    // 1,t1: cfg { server 1 }
    // 2,t6: no op
    // 3,t6: transitional cfg { server 1 } -> { server 2 }
    // 4,t6: cfg { server 2 }
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    entry1.set_term(6);
    *entry1.mutable_configuration() = desc(
        "prev_configuration {"
        "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
        "}"
        "next_configuration {"
            "servers { server_id: 2, addresses: '127.0.0.1:5255' }"
        "}");
    consensus->append({&entry1});
    drainDiskQueue(*consensus);
    getPeer(2)->matchIndex = 3;
    consensus->advanceCommitIndex();
    EXPECT_EQ(3U, consensus->commitIndex);
    EXPECT_EQ(4U, consensus->log->getLastLogIndex());
    EXPECT_EQ(State::LEADER, consensus->state);

    getPeer(2)->matchIndex = 4;
    consensus->advanceCommitIndex();
    EXPECT_EQ(4U, consensus->commitIndex);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
}

TEST_F(ServerRaftConsensusTest, advanceCommitIndex_commitTransitionToSelf)
{
    // Log:
    // 1,t1: cfg { server 1:5254 }
    // 2,t6: no op
    // 3,t6: transitional cfg { server 1:5254 } -> { server 1:5256 }
    // 4,t6: cfg { server 1:5256 }
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    EXPECT_EQ(State::LEADER, consensus->state);
    entry3.set_term(6);
    consensus->append({&entry3});
    drainDiskQueue(*consensus);
    EXPECT_EQ(4U, consensus->configuration->localServer->lastSyncedIndex);
    EXPECT_EQ(4U, consensus->commitIndex);
    EXPECT_EQ(4U, consensus->log->getLastLogIndex());
    const Log::Entry& l3 = consensus->log->getEntry(4);
    EXPECT_EQ(Protocol::Raft::EntryType::CONFIGURATION, l3.type());
    EXPECT_EQ("prev_configuration {"
                  "servers { server_id: 1, addresses: '127.0.0.1:5256' }"
              "}",
              l3.configuration());
}

TEST_F(ServerRaftConsensusTest, append)
{
    // Log:
    // 1,t1: cfg { server 1 }
    // 2,t2: "hello"
    // 3,t?: nop
    init();
    consensus->stepDown(5);
    consensus->append({});
    consensus->append({&entry1, &entry2});
    EXPECT_FALSE(consensus->logSyncQueued);
    EXPECT_EQ(1U, consensus->configuration->id);
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_EQ((std::vector<uint64_t>{1}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                                descriptions));
    EXPECT_EQ(d, consensus->configurationManager->descriptions.at(1));

    // leaders put onto diskQueue rather than syncing inline
    consensus->startNewElection();
    EXPECT_TRUE(consensus->logSyncQueued);
}

// used in AppendEntries tests
class ServerRaftConsensusPATest : public ServerRaftConsensusPTest {
    ServerRaftConsensusPATest()
        : peer()
        , request()
        , response()
    {
        // Log:
        // 1,t1: cfg { server 1 }
        // 2,t2: "hello"
        // 3,t6: no-op
        // 4,t6: cfg { server 1,2 }
        init();
        consensus->append({&entry1});
        consensus->append({&entry2});
        consensus->stepDown(5);
        consensus->startNewElection();
        drainDiskQueue(*consensus);
        entry5.set_term(6);
        consensus->append({&entry5});
        drainDiskQueue(*consensus);
        EXPECT_EQ(State::LEADER, consensus->state);
        peer = getPeerRef(2);

        // For some reason or other, these tests are written to assume the
        // leader has determined that peer and it diverge on the first log
        // entry.
        EXPECT_EQ(5U, peer->nextIndex);
        EXPECT_TRUE(peer->suppressBulkData);
        peer->nextIndex = 1;
        peer->suppressBulkData = false;

        request.set_server_id(1);
        request.set_term(6);
        request.set_prev_log_term(0);
        request.set_prev_log_index(0);
        request.set_commit_index(3);
        Protocol::Raft::Entry* e1 = request.add_entries();
        e1->set_term(1);
        e1->set_cluster_time(0);
        e1->set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *e1->mutable_configuration() = entry1.configuration();
        Protocol::Raft::Entry* e2 = request.add_entries();
        e2->set_term(2);
        e2->set_cluster_time(0);
        e2->set_type(Protocol::Raft::EntryType::DATA);
        e2->set_data(entry2.data());
        Protocol::Raft::Entry* enop = request.add_entries();
        enop->set_term(6);
        enop->set_cluster_time(0);
        enop->set_type(Protocol::Raft::EntryType::NOOP);
        Protocol::Raft::Entry* e3 = request.add_entries();
        e3->set_term(6);
        e3->set_cluster_time(0);
        e3->set_type(Protocol::Raft::EntryType::CONFIGURATION);
        *e3->mutable_configuration() = entry5.configuration();

        response.set_term(6);
        response.set_success(true);
    }

    std::shared_ptr<Peer> peer;
    Protocol::Raft::AppendEntries::Request request;
    Protocol::Raft::AppendEntries::Response response;
};

TEST_F(ServerRaftConsensusPATest, appendEntries_rpcFailed)
{
    peerService->closeSession(Protocol::Raft::OpCode::APPEND_ENTRIES, request);
    // expect warning
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "ERROR"}
    });
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_LT(Clock::now(), peer->backoffUntil);
    EXPECT_EQ(0U, peer->matchIndex);
}

// Mostly a test for packEntries now that that function has been split out of
// AppendEntries.
TEST_F(ServerRaftConsensusPATest, appendEntries_limitSizeAndIgnoreResult)
{
    consensus->SOFT_RPC_SIZE_LIMIT = 1;
    request.mutable_entries()->RemoveLast();
    request.mutable_entries()->RemoveLast();
    request.mutable_entries()->RemoveLast();
    request.set_commit_index(1);
    peer->exiting = true;
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(0U, peer->matchIndex);
}

// Mostly a test for packEntries now that that function has been split out of
// AppendEntries.
//
// In #160, an entry that wouldn't fit in the request didn't cause the loop to
// exit. If an entry later in the log would fit in the request, it would be
// added to the request invalidly. This test targets that specific behavior (it
// fails before the 'break' statement was added and passes after).
TEST_F(ServerRaftConsensusPATest, appendEntries_limitSizeRegression)
{
    // First we determine the sizes contributed by the various entries in
    // 'request'.
    Protocol::Raft::AppendEntries::Request r2;
    r2.CopyFrom(request);
    r2.clear_entries();
    // size of request with no entries
    uint64_t baseSize = uint64_t(r2.ByteSize());
    std::vector<uint64_t> entrySizes; // size of each entry
    uint64_t totalSize = baseSize; // size of full request
    for (int i = 0; i < request.entries_size(); ++i) {
        *r2.add_entries() = request.entries(i);
        uint64_t entrySize = uint64_t(r2.ByteSize()) - baseSize;
        entrySizes.push_back(entrySize);
        totalSize += entrySize;
        r2.clear_entries();
    }
    EXPECT_EQ((std::vector<uint64_t> { 32, 15, 8, 52 }),
              entrySizes);
    EXPECT_EQ(totalSize, uint64_t(request.ByteSize()));

    // We now cap request sizes so that entry 2 doesn't fit but entry 3 would.
    consensus->SOFT_RPC_SIZE_LIMIT = (baseSize +
                                      entrySizes.at(0) +
                                      entrySizes.at(2) +
                                      5);
    *r2.add_entries() = request.entries(0);
    r2.set_commit_index(1);
    peer->exiting = true;
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       r2, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(0U, peer->matchIndex);
}

TEST_F(ServerRaftConsensusPATest, appendEntries_suppressBulkData)
{
    peer->suppressBulkData = true;
    request.mutable_entries()->Clear();
    request.set_commit_index(0);
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_FALSE(peer->suppressBulkData);
}

TEST_F(ServerRaftConsensusPATest, appendEntries_termChanged)
{
    peerService->runArbitraryCode(
            Protocol::Raft::OpCode::APPEND_ENTRIES,
            request,
            std::make_shared<BumpTermAndReply>(*consensus, response));
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(TimePoint::min(), peer->backoffUntil);
    EXPECT_EQ(0U, peer->matchIndex);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
}


TEST_F(ServerRaftConsensusPATest, appendEntries_termStale)
{
    response.set_term(10);
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(0U, peer->matchIndex);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ(10U, consensus->currentTerm);
}

TEST_F(ServerRaftConsensusPATest, appendEntries_ok)
{
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(consensus->currentEpoch, peer->lastAckEpoch);
    EXPECT_EQ(4U, peer->matchIndex);
    EXPECT_EQ(Clock::mockValue + consensus->HEARTBEAT_PERIOD,
              peer->nextHeartbeatTime);

    // TODO(ongaro): test catchup code
}

TEST_F(ServerRaftConsensusPATest, appendEntries_mismatch)
{
    // if the follower's log is too short, need to decrement nextIndex
    std::unique_lock<Mutex> lockGuard(consensus->mutex);

    // decrementing by one
    peer->nextIndex = 5;
    request.set_prev_log_index(4);
    request.set_prev_log_term(6);
    request.clear_entries();
    response.set_success(false);
    response.set_last_log_index(300);
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(4U, peer->nextIndex);

    // capping to last log index + 1
    peer->nextIndex = 5;
    response.set_last_log_index(0);
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_EQ(1U, peer->nextIndex);
}

TEST_F(ServerRaftConsensusPATest, appendEntries_serverCapabilities)
{
    auto& cap = *response.mutable_server_capabilities();
    cap.set_min_supported_state_machine_version(10);
    cap.set_max_supported_state_machine_version(20);
    peerService->reply(Protocol::Raft::OpCode::APPEND_ENTRIES,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->appendEntries(lockGuard, *peer);
    EXPECT_TRUE(peer->haveStateMachineSupportedVersions);
    EXPECT_EQ(10U, peer->minStateMachineVersion);
    EXPECT_EQ(20U, peer->maxStateMachineVersion);
}

// test that installSnapshot gets called
TEST_F(ServerRaftConsensusPATest, appendEntries_snapshot)
{
    std::unique_lock<Mutex> lockGuard(consensus->mutex);

    // nextIndex < log start
    consensus->log->truncatePrefix(2);
    EXPECT_EQ(2U, consensus->log->getLogStartIndex());
    peer->nextIndex = 1;
    EXPECT_DEATH(consensus->appendEntries(lockGuard, *peer),
                 "Could not open .*snapshot");

    // nextIndex >= log start but prev needed for term
    peer->nextIndex = 2;
    EXPECT_DEATH(consensus->appendEntries(lockGuard, *peer),
                 "Could not open .*snapshot");

    // TODO(ongaro): should also test the various ways prevLogTerm can be set,
    // but it's not easily testable
}

// used in InstallSnapshot tests
class ServerRaftConsensusPSTest : public ServerRaftConsensusPTest {
    ServerRaftConsensusPSTest()
        : peer()
        , request()
        , response()
    {
        init();
        consensus->append({&entry1});
        consensus->stepDown(4);
        consensus->startNewElection();
        drainDiskQueue(*consensus);
        consensus->append({&entry5});
        drainDiskQueue(*consensus);
        EXPECT_EQ(State::LEADER, consensus->state);
        EXPECT_EQ(5U, consensus->currentTerm);
        peer = getPeerRef(2);

        // First create a snapshot file on disk.
        // Note that this one doesn't have a Raft header.
        Storage::SnapshotFile::Writer w(consensus->storageLayout);
        w.writeRaw("hello, world!", 13);
        w.save();
        consensus->lastSnapshotIndex = 2;

        request.set_server_id(1);
        request.set_term(5);
        request.set_last_snapshot_index(2);
        request.set_byte_offset(0);
        request.set_data("hello, world!");
        request.set_done(true);
        request.set_version(2);

        response.set_term(5);
    }

    std::shared_ptr<Peer> peer;
    Protocol::Raft::InstallSnapshot::Request request;
    Protocol::Raft::InstallSnapshot::Response response;
};

TEST_F(ServerRaftConsensusPSTest, installSnapshot_rpcFailed)
{
    peer->suppressBulkData = false;
    peerService->closeSession(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                              request);
    // expect warning
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "ERROR"}
    });
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_LT(Clock::now(), peer->backoffUntil);
    EXPECT_EQ(0U, peer->snapshotFileOffset);
}


TEST_F(ServerRaftConsensusPSTest, installSnapshot_termChanged)
{
    peer->suppressBulkData = false;
    peerService->runArbitraryCode(
            Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
            request,
            std::make_shared<BumpTermAndReply>(*consensus, response));
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_EQ(TimePoint::min(), peer->backoffUntil);
    EXPECT_EQ(0U, peer->snapshotFileOffset);
}

TEST_F(ServerRaftConsensusPSTest, installSnapshot_termStale)
{
    peer->suppressBulkData = false;
    response.set_term(10);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_EQ(0U, peer->snapshotFileOffset);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ(10U, consensus->currentTerm);
}

TEST_F(ServerRaftConsensusPSTest, installSnapshot_ok)
{
    peer->suppressBulkData = false;
    consensus->SOFT_RPC_SIZE_LIMIT = 7;
    request.set_data("hello, ");
    request.set_done(false);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    request.set_byte_offset(7);
    request.set_data("world!");
    request.set_done(true);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->installSnapshot(lockGuard, *peer);
    // make sure we don't use an updated lastSnapshotIndex value
    consensus->lastSnapshotIndex = 1;
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_EQ(2U, peer->matchIndex);
    EXPECT_EQ(3U, peer->nextIndex);
    EXPECT_FALSE(peer->snapshotFile);
    EXPECT_EQ(0U, peer->snapshotFileOffset);
    EXPECT_EQ(0U, peer->lastSnapshotIndex);
    EXPECT_EQ(consensus->currentEpoch, peer->lastAckEpoch);
    EXPECT_EQ(Clock::mockValue + consensus->HEARTBEAT_PERIOD,
              peer->nextHeartbeatTime);
}

TEST_F(ServerRaftConsensusPSTest, installSnapshot_suppressBulkData)
{
    peer->suppressBulkData = true;
    consensus->SOFT_RPC_SIZE_LIMIT = 7;
    request.set_data("");
    request.set_done(false);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    request.set_data("hello, ");
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_FALSE(peer->suppressBulkData);
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_FALSE(peer->suppressBulkData);
}

TEST_F(ServerRaftConsensusPSTest, installSnapshot_notAllBytesStored)
{
    peer->suppressBulkData = false;
    consensus->SOFT_RPC_SIZE_LIMIT = 7;
    request.set_data("hello, ");
    request.set_done(false);
    response.set_bytes_stored(4);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    request.set_byte_offset(4);
    request.set_data("o, worl");
    response.set_bytes_stored(0);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    request.set_byte_offset(0);
    request.set_data("hello, ");
    response.set_bytes_stored(7);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);
    request.set_byte_offset(7);
    request.set_data("world!");
    request.set_done(true);
    response.set_bytes_stored(13);
    peerService->reply(Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                       request, response);

    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->installSnapshot(lockGuard, *peer);
    consensus->installSnapshot(lockGuard, *peer);
    consensus->installSnapshot(lockGuard, *peer);
    consensus->installSnapshot(lockGuard, *peer);
    EXPECT_EQ(2U, peer->matchIndex);
}


TEST_F(ServerRaftConsensusTest, becomeLeader)
{
    init();
    consensus->stepDown(5);
    entry5.set_cluster_time(60);
    consensus->clusterClock.newEpoch(60);
    consensus->append({&entry5});
    EXPECT_EQ(5U, consensus->currentTerm);
    consensus->startNewElection();
    Peer& peer = *getPeer(2);
    peer.requestVoteDone = true;
    peer.haveVote_ = true;
    Clock::mockValue += std::chrono::hours(10);
    consensus->becomeLeader();
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_EQ(6U, consensus->currentTerm);
    EXPECT_EQ(1U, consensus->leaderId);
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_EQ(2U, peer.nextIndex);
    EXPECT_EQ(1U, consensus->configuration->localServer->lastSyncedIndex);
    EXPECT_EQ(0U, consensus->commitIndex);
    const Log::Entry& nop = consensus->log->getEntry(2);
    EXPECT_EQ(6U, nop.term());
    EXPECT_EQ(Protocol::Raft::EntryType::NOOP, nop.type());
    EXPECT_EQ(TimePoint::max(), consensus->startElectionAt);
    EXPECT_EQ(60U, consensus->clusterClock.clusterTimeAtEpoch);
    EXPECT_EQ(Clock::mockValue, consensus->clusterClock.localTimeAtEpoch);

    drainDiskQueue(*consensus);
    EXPECT_EQ(2U, consensus->configuration->localServer->lastSyncedIndex);

    // TODO(ongaro): add becomeLeader test when not part of current config?
}

TEST_F(ServerRaftConsensusTest, discardUnneededEntries)
{
    init();
    consensus->discardUnneededEntries();
    EXPECT_EQ(1U, consensus->log->getLogStartIndex());
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(2);
    uint32_t d = 0xdeadbeef;
    writer->writeRaw(&d, sizeof(d));
    consensus->snapshotDone(2, std::move(writer));
    consensus->discardUnneededEntries();
    EXPECT_EQ(3U, consensus->log->getLogStartIndex());
}

TEST_F(ServerRaftConsensusTest, getLastLogTerm)
{
    init();
    // empty log, no snapshot
    EXPECT_EQ(0U, consensus->getLastLogTerm());
    // log
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    EXPECT_EQ(1U, consensus->getLastLogTerm());
    // snapshot only
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(2);
    uint32_t d = 0xdeadbeef;
    writer->writeRaw(&d, sizeof(d));
    consensus->snapshotDone(2, std::move(writer));
    EXPECT_LT(consensus->log->getLastLogIndex(),
              consensus->log->getLogStartIndex());
    EXPECT_EQ(1U, consensus->getLastLogTerm());
}

TEST_F(ServerRaftConsensusTest, interruptAll)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->append({&entry5});
    consensus->stateChanged.notificationCount = 0;
    consensus->interruptAll();
    Peer& peer = *getPeer(2);
    EXPECT_EQ("RPC canceled by user", peer.rpc.getErrorMessage());
    EXPECT_EQ(1U, consensus->stateChanged.notificationCount);
}

// packEntries used to be part of appendEntries. The tests
// appendEntries_limitSizeAndIgnoreResult and appendEntries_limitSizeRegression
// mostly target the packEntries functionality.

TEST_F(ServerRaftConsensusTest, packEntries)
{
    init();
    consensus->stepDown(5);

    // limit by log length (of 0)
    Protocol::Raft::AppendEntries::Request request;
    EXPECT_EQ(0U, consensus->packEntries(1U, request));
    request.clear_entries();

    // limit by log length (of 2)
    consensus->append({&entry1});
    consensus->append({&entry2});
    EXPECT_EQ(2U, consensus->packEntries(1U, request));
    request.clear_entries();

    // limit by number of log entries
    for (uint64_t i = 0; i < 128; ++i)
        consensus->append({&entry2});
    consensus->SOFT_RPC_SIZE_LIMIT = 1024 * 1024;
    consensus->MAX_LOG_ENTRIES_PER_REQUEST = 32;
    EXPECT_EQ(32U, consensus->packEntries(3U, request));
    request.clear_entries();
    consensus->MAX_LOG_ENTRIES_PER_REQUEST = 5000;

    // limit by number of bytes
    consensus->SOFT_RPC_SIZE_LIMIT = 1024;
    uint64_t n = consensus->packEntries(3U, request);
    EXPECT_GT(5000U, n);
    EXPECT_LT(0U, n);
    EXPECT_GE(1024, request.ByteSize());
    *request.add_entries() = consensus->log->getEntry(3);
    EXPECT_LE(1024, request.ByteSize());
    request.clear_entries();

    // one entry is allowed even if it's too big
    consensus->SOFT_RPC_SIZE_LIMIT = 1;
    EXPECT_EQ(1U, consensus->packEntries(3U, request));
}

TEST_F(ServerRaftConsensusTest, readSnapshot)
{
    init();

    // snapshot not found
    consensus->readSnapshot();
    EXPECT_EQ(0U, consensus->lastSnapshotIndex);
    EXPECT_EQ(0U, consensus->lastSnapshotTerm);
    EXPECT_FALSE(bool(consensus->snapshotReader));

    // snapshot found
    entry1.set_cluster_time(10);
    consensus->clusterClock.newEpoch(10);
    consensus->append({&entry1});
    consensus->clusterClock.newEpoch(20);
    consensus->currentTerm = 1;
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    EXPECT_EQ(2U, consensus->commitIndex);

    std::unique_ptr<Storage::MemoryLog> log2(new Storage::MemoryLog());
    {
        Log::Entry e1, e2;
        e1 = consensus->log->getEntry(1);
        e2 = consensus->log->getEntry(2);
        log2->append({&e1, &e2});
    }

    consensus->beginSnapshot(2)->save();
    consensus->commitIndex = 0;
    consensus->configurationManager->descriptions.clear();
    consensus->readSnapshot();
    EXPECT_EQ(2U, consensus->lastSnapshotIndex);
    EXPECT_EQ(2U, consensus->lastSnapshotTerm);
    EXPECT_EQ(2U, consensus->commitIndex);
    EXPECT_EQ(20U, consensus->lastSnapshotClusterTime);
    // Don't really know exactly how big the snapshot will be in bytes, but
    // between 10 and 1K seems reasonable.
    EXPECT_LT(10U, consensus->lastSnapshotBytes);
    EXPECT_GT(1024U, consensus->lastSnapshotBytes);
    EXPECT_TRUE(bool(consensus->snapshotReader));
    EXPECT_EQ((std::vector<uint64_t>{1}),
              Core::STLUtil::getKeys(consensus->configurationManager->
                                                            descriptions));
    EXPECT_EQ(d, consensus->configurationManager->descriptions.at(1));
    EXPECT_EQ(3U, consensus->log->getLogStartIndex());
    EXPECT_EQ(20U, consensus->clusterClock.clusterTimeAtEpoch);

    // does not affect commitIndex if done again
    entry2.set_cluster_time(30);
    consensus->clusterClock.newEpoch(30);
    consensus->append({&entry2});
    drainDiskQueue(*consensus);
    EXPECT_EQ(3U, consensus->commitIndex);
    consensus->readSnapshot();
    EXPECT_EQ(2U, consensus->lastSnapshotIndex);
    EXPECT_EQ(2U, consensus->lastSnapshotTerm);
    EXPECT_EQ(3U, consensus->commitIndex);
    EXPECT_TRUE(bool(consensus->snapshotReader));

    // truncates the log if it does not agree with the snapshot
    EXPECT_EQ(3U, consensus->log->getLogStartIndex());
    EXPECT_EQ(3U, consensus->log->getLastLogIndex());

    log2->metadata = consensus->log->metadata;
    consensus->log.reset(log2.release());
    consensus->commitIndex = 0;
    consensus->configuration->localServer->lastSyncedIndex = 0;
    consensus->clusterClock.newEpoch(20);
    consensus->lastSnapshotIndex = 0;
    EXPECT_EQ(1U, consensus->log->getLogStartIndex());
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    consensus->readSnapshot();
    EXPECT_EQ(3U, consensus->log->getLogStartIndex());
    EXPECT_EQ(2U, consensus->log->getLastLogIndex());
    EXPECT_EQ(2U, consensus->commitIndex);
}

TEST_F(ServerRaftConsensusTest, readSnapshot_incompleteLogPrefix)
{
    // snapshot not found, needed to have complete log
    init();
    EXPECT_DEATH({ consensus->log->truncatePrefix(2);
                   consensus->readSnapshot(); },
                 "corrupt disk state");
}

TEST_F(ServerRaftConsensusTest, readSnapshot_emptyFile)
{
    init();
    Storage::SnapshotFile::Writer writer(consensus->storageLayout);
    writer.save();
    EXPECT_DEATH({ consensus->readSnapshot(); },
                 "completely empty");
}

TEST_F(ServerRaftConsensusTest, readSnapshot_unknownVersion)
{
    init();
    Storage::SnapshotFile::Writer writer(consensus->storageLayout);
    uint8_t version = 2;
    writer.writeRaw(&version, sizeof(version));
    writer.save();
    EXPECT_DEATH({ consensus->readSnapshot(); },
                 "Snapshot format version read was 2, but this code can only "
                 "read version 1");
}

TEST_F(ServerRaftConsensusTest, replicateEntry_notLeader)
{
    init();
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    EXPECT_EQ(ClientResult::NOT_LEADER,
              consensus->replicateEntry(entry2, lockGuard).first);
}

TEST_F(ServerRaftConsensusTest, replicateEntry_okJustUs)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    consensus->leaderDiskThread =
        std::thread(&RaftConsensus::leaderDiskThreadMain, consensus.get());
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    std::pair<ClientResult, uint64_t> result =
        consensus->replicateEntry(entry2, lockGuard);
    EXPECT_EQ(ClientResult::SUCCESS, result.first);
    // 1: entry1, 2: no-op, 3: entry2
    EXPECT_EQ(3U, result.second);
}

TEST_F(ServerRaftConsensusTest, replicateEntry_termChanged)
{
    init();
    consensus->stepDown(4);
    consensus->append({&entry1});
    consensus->startNewElection();
    consensus->append({&entry5});
    EXPECT_EQ(State::LEADER, consensus->state);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->stateChanged.callback = std::bind(&RaftConsensus::stepDown,
                                                 consensus.get(), 7);
    EXPECT_EQ(ClientResult::NOT_LEADER,
              consensus->replicateEntry(entry2, lockGuard).first);
}

TEST_F(ServerRaftConsensusPTest, requestVote_rpcFailed)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry5});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    Peer& peer = *getPeer(2);

    Protocol::Raft::RequestVote::Request request;
    request.set_server_id(1);
    request.set_term(6);
    request.set_last_log_term(5);
    request.set_last_log_index(1);

    peerService->closeSession(Protocol::Raft::OpCode::REQUEST_VOTE, request);
    // expect warning
    LogCabin::Core::Debug::setLogPolicy({
        {"Server/RaftConsensus.cc", "ERROR"}
    });
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->requestVote(lockGuard, peer);
    EXPECT_LT(Clock::now(), peer.backoffUntil);
    EXPECT_FALSE(peer.requestVoteDone);
}

TEST_F(ServerRaftConsensusPTest, requestVote_ignoreResult)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry5});
    // don't become candidate so the response is ignored
    Peer& peer = *getPeer(2);

    Protocol::Raft::RequestVote::Request request;
    request.set_server_id(1);
    request.set_term(5);
    request.set_last_log_term(5);
    request.set_last_log_index(1);

    Protocol::Raft::RequestVote::Response response;
    response.set_term(5);
    response.set_granted(true);

    peerService->reply(Protocol::Raft::OpCode::REQUEST_VOTE,
                       request, response);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->requestVote(lockGuard, peer);
    EXPECT_FALSE(peer.requestVoteDone);
}

TEST_F(ServerRaftConsensusPTest, requestVote_termStale)
{
    // Log:
    // 1,t1: cfg { server 1 }
    // 2,t6: no op
    // 3,t6: transitional cfg { server 1 } -> { server 2 }
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection(); // become leader
    *entry1.mutable_configuration() = desc(d4);
    entry1.set_term(6);
    consensus->append({&entry1});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    Peer& peer = *getPeer(2);

    Protocol::Raft::RequestVote::Request request;
    request.set_server_id(1);
    request.set_term(7);
    request.set_last_log_term(6);
    request.set_last_log_index(3);

    Protocol::Raft::RequestVote::Response response;
    response.set_term(8);
    response.set_granted(false);

    peerService->reply(Protocol::Raft::OpCode::REQUEST_VOTE,
                       request, response);
    TimePoint oldStartElectionAt = consensus->startElectionAt;
    Clock::mockValue += milliseconds(2);
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    consensus->requestVote(lockGuard, peer);
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    // check that the election timer was not reset
    EXPECT_EQ(oldStartElectionAt, consensus->startElectionAt);
    EXPECT_EQ(8U, consensus->currentTerm);
}

TEST_F(ServerRaftConsensusPTest, requestVote_termOkAsLeader)
{
    // Log:
    // 1,t1: cfg { server 1,2,3,4,5 }
    // 2,t2: "hello"
    // 3,t2: "hello"
    // 4,t2: "hello"
    init();
    consensus->stepDown(5);
    *entry1.mutable_configuration() = desc(
        "prev_configuration {"
        "    servers { server_id: 1, addresses: '127.0.0.1:5254' }"
        "    servers { server_id: 2, addresses: '127.0.0.1:5255' }"
        "    servers { server_id: 3, addresses: '127.0.0.1:5255' }"
        "    servers { server_id: 4, addresses: '127.0.0.1:5255' }"
        "    servers { server_id: 5, addresses: '127.0.0.1:5255' }"
        "}");
    consensus->append({&entry1});
    consensus->append({&entry2});
    consensus->append({&entry2});
    consensus->append({&entry2});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    consensus->currentEpoch = 1000;
    Peer& peer2 = *getPeer(2);
    Peer& peer3 = *getPeer(3);
    Peer& peer4 = *getPeer(4);

    std::unique_lock<Mutex> lockGuard(consensus->mutex);

    // 1. Get response from peer2 but don't get its vote.
    Protocol::Raft::RequestVote::Request request;
    request.set_server_id(1);
    request.set_term(6);
    request.set_last_log_term(2);
    request.set_last_log_index(4);

    Protocol::Raft::RequestVote::Response response;
    response.set_term(6);
    response.set_granted(false);

    peerService->reply(Protocol::Raft::OpCode::REQUEST_VOTE,
                       request, response);
    consensus->requestVote(lockGuard, peer2);
    EXPECT_TRUE(peer2.requestVoteDone);
    EXPECT_EQ(1000U, peer2.lastAckEpoch);
    EXPECT_EQ(State::CANDIDATE, consensus->state);

    // 2. Get vote from peer3, still a candidate
    response.set_granted(true);
    peerService->reply(Protocol::Raft::OpCode::REQUEST_VOTE,
                       request, response);
    consensus->requestVote(lockGuard, peer3);
    EXPECT_TRUE(peer3.requestVoteDone);
    EXPECT_EQ(1000U, peer3.lastAckEpoch);
    EXPECT_EQ(State::CANDIDATE, consensus->state);

    // 3. Get vote from peer4, become leader
    peerService->reply(Protocol::Raft::OpCode::REQUEST_VOTE,
                       request, response);
    consensus->requestVote(lockGuard, peer4);
    EXPECT_TRUE(peer4.requestVoteDone);
    EXPECT_EQ(1000U, peer4.lastAckEpoch);
    EXPECT_EQ(State::LEADER, consensus->state);
}

TEST_F(ServerRaftConsensusTest, setElectionTimer)
{
    // TODO(ongaro): seed the random number generator and make sure the values
    // look sane
    init();
    for (uint64_t i = 0; i < 100; ++i) {
        consensus->setElectionTimer();
        EXPECT_LE(Clock::now() + consensus->ELECTION_TIMEOUT,
                  consensus->startElectionAt);
        EXPECT_GE(Clock::now() + consensus->ELECTION_TIMEOUT * 2,
                  consensus->startElectionAt);
    }
}

TEST_F(ServerRaftConsensusTest, startNewElection)
{
    init();

    // no configuration yet -> no op
    consensus->startNewElection();
    EXPECT_EQ(State::FOLLOWER, consensus->state);
    EXPECT_EQ(0U, consensus->currentTerm);
    EXPECT_LT(Clock::now(), consensus->startElectionAt);
    EXPECT_GT(Clock::now() + consensus->ELECTION_TIMEOUT * 2,
              consensus->startElectionAt);

    // need other votes to win
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->append({&entry5});

    consensus->snapshotWriter.reset(
        new Storage::SnapshotFile::Writer(consensus->storageLayout));
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
    EXPECT_EQ(6U, consensus->currentTerm);
    EXPECT_EQ(0U, consensus->leaderId);
    EXPECT_EQ(1U, consensus->votedFor);
    EXPECT_LT(Clock::now(), consensus->startElectionAt);
    EXPECT_GT(Clock::now() + consensus->ELECTION_TIMEOUT * 2,
              consensus->startElectionAt);
    EXPECT_FALSE(bool(consensus->snapshotWriter));

    // already won
    consensus->stepDown(7);
    entry1.set_term(7);
    consensus->append({&entry1});
    consensus->startNewElection();
    EXPECT_EQ(State::LEADER, consensus->state);

    // not part of current configuration
    consensus->stepDown(10);
    entry1.set_term(9);
    *entry1.mutable_configuration() = desc(
        "prev_configuration {"
            "servers { server_id: 2, addresses: '127.0.0.1:5256' }"
        "}");
    consensus->append({&entry1});
    consensus->startNewElection();
    EXPECT_EQ(State::CANDIDATE, consensus->state);
}

TEST_F(ServerRaftConsensusTest, stepDown)
{
    init();

    // set up
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    consensus->configuration->setStagingServers(sdesc(""));
    consensus->stateChanged.notify_all();
    EXPECT_NE(0U, consensus->leaderId);
    EXPECT_NE(0U, consensus->votedFor);
    EXPECT_EQ(TimePoint::max(), consensus->startElectionAt);
    EXPECT_EQ(Configuration::State::STAGING, consensus->configuration->state);

    // from leader to new term
    EXPECT_TRUE(consensus->logSyncQueued);
    consensus->stepDown(10);
    EXPECT_EQ(0U, consensus->leaderId);
    EXPECT_EQ(0U, consensus->votedFor);
    EXPECT_EQ(Configuration::State::STABLE, consensus->configuration->state);
    EXPECT_LT(Clock::now(), consensus->startElectionAt);
    EXPECT_GT(Clock::now() + consensus->ELECTION_TIMEOUT * 2,
              consensus->startElectionAt);
    EXPECT_FALSE(consensus->logSyncQueued);

    // from candidate to same term
    entry5.set_term(6);
    consensus->append({&entry5});
    consensus->startNewElection();
    consensus->leaderId = 3;
    TimePoint oldStartElectionAt = consensus->startElectionAt;
    Clock::mockValue += milliseconds(2);
    consensus->stepDown(consensus->currentTerm);
    EXPECT_NE(0U, consensus->leaderId);
    EXPECT_NE(0U, consensus->votedFor);
    EXPECT_EQ(oldStartElectionAt, consensus->startElectionAt);

    // from follower to new term
    consensus->snapshotWriter.reset(
        new Storage::SnapshotFile::Writer(consensus->storageLayout));
    consensus->stepDown(consensus->currentTerm + 1);
    EXPECT_EQ(oldStartElectionAt, consensus->startElectionAt);
    EXPECT_FALSE(bool(consensus->snapshotWriter));
}

TEST_F(ServerRaftConsensusTest, updateLogMetadata)
{
    init();
    consensus->stepDown(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    consensus->updateLogMetadata();
    EXPECT_EQ(6U, consensus->log->metadata.current_term());
    EXPECT_EQ(1U, consensus->log->metadata.voted_for());
}

// used in upToDateLeader
class UpToDateLeaderHelper {
    explicit UpToDateLeaderHelper(RaftConsensus* consensus)
        : consensus(consensus)
        , iter(1)
    {
    }
    void operator()() {
        Server* server = consensus->configuration->knownServers.at(2).get();
        Peer* peer = dynamic_cast<Peer*>(server);
        if (iter == 1) {
            peer->lastAckEpoch = consensus->currentEpoch;
        } else if (iter == 2) {
            peer->matchIndex = 4;
            consensus->advanceCommitIndex();
        } else {
            FAIL();
        }
        ++iter;
    }
    RaftConsensus* consensus;
    uint64_t iter;
};

TEST_F(ServerRaftConsensusTest, upToDateLeader)
{
    // Log:
    // 1,t5: config { s1 }
    // 2,t6: no op
    // 3,t6: config { s1, s2 }
    // 4,t7: no op
    init();
    std::unique_lock<Mutex> lockGuard(consensus->mutex);
    // not leader -> false
    EXPECT_FALSE(consensus->upToDateLeader(lockGuard));
    consensus->stepDown(5);
    entry1.set_term(5);
    consensus->append({&entry1});
    consensus->startNewElection();
    drainDiskQueue(*consensus);
    // leader of just self -> true
    EXPECT_EQ(State::LEADER, consensus->state);
    EXPECT_TRUE(consensus->upToDateLeader(lockGuard));
    // snapshot and discard log -> true
    lockGuard.unlock();
    std::unique_ptr<Storage::SnapshotFile::Writer> writer =
        consensus->beginSnapshot(2);
    uint32_t d = 0xdeadbeef;
    writer->writeRaw(&d, sizeof(d));
    consensus->snapshotDone(2, std::move(writer));
    consensus->log->truncatePrefix(3);
    consensus->configurationManager->truncatePrefix(3);
    consensus->stateChanged.notify_all();
    lockGuard.lock();
    EXPECT_TRUE(consensus->upToDateLeader(lockGuard));
    // leader of non-trivial cluster -> wait, then true
    entry5.set_term(6);
    consensus->append({&entry5});
    consensus->startNewElection();
    consensus->becomeLeader();
    drainDiskQueue(*consensus);
    Peer* peer = getPeer(2);
    UpToDateLeaderHelper helper(consensus.get());
    consensus->stateChanged.callback = std::ref(helper);
    peer->nextHeartbeatTime = TimePoint::max();
    EXPECT_TRUE(consensus->upToDateLeader(lockGuard));
    EXPECT_EQ(Clock::now(),
              peer->nextHeartbeatTime);
    EXPECT_EQ(3U, helper.iter);
}

// This tests an old bug in which nextIndex was not set properly for servers
// that were just added to the configuration.
TEST_F(ServerRaftConsensusTest, regression_nextIndexForNewServer)
{
    // Log:
    // 1,t1: config { s1 }
    // 2,t5: no op
    // 3,t5: config { s1, s2 }
    init();
    consensus->append({&entry1});
    consensus->stepDown(4);
    consensus->startNewElection();
    consensus->append({&entry5});
    EXPECT_EQ(4U, getPeer(2)->nextIndex);
    EXPECT_TRUE(getPeer(2)->suppressBulkData);
}


} // namespace LogCabin::Server::<anonymous>
} // namespace LogCabin::Server
} // namespace LogCabin
