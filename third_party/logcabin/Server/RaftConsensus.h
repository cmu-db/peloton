/* Copyright (c) 2012-2014 Stanford University
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

#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_map>

#include "build/Protocol/Client.pb.h"
#include "build/Protocol/Raft.pb.h"
#include "build/Protocol/ServerStats.pb.h"
#include "build/Server/SnapshotStats.pb.h"
#include "Client/SessionManager.h"
#include "Core/CompatAtomic.h"
#include "Core/ConditionVariable.h"
#include "Core/Mutex.h"
#include "Core/Time.h"
#include "RPC/ClientRPC.h"
#include "Storage/Layout.h"
#include "Storage/Log.h"
#include "Storage/SnapshotFile.h"

#ifndef LOGCABIN_SERVER_RAFTCONSENSUS_H
#define LOGCABIN_SERVER_RAFTCONSENSUS_H

namespace LogCabin {

// forward declarations
namespace Event {
class Loop;
}
namespace RPC {
class ClientSession;
}

namespace Server {

// forward declaration
class Globals;

// forward declaration
class RaftConsensus;

namespace RaftConsensusInternal {


class Invariants {
  public:
    explicit Invariants(RaftConsensus&);
    ~Invariants();
    void checkAll();
  private:
    void checkBasic();
    void checkPeerBasic();
    void checkDelta();
    void checkPeerDelta();

    const RaftConsensus& consensus;
    uint64_t errors;
    struct ConsensusSnapshot;
    std::unique_ptr<ConsensusSnapshot> previous;
};


/**
 * True if this should actually spawn threads, false otherwise.
 * Normally set to true, but many unit tests set this to false.
 */
extern bool startThreads;

/**
 * Reads the current time. This will refer to the best clock available on our
 * system, which may or may not be monotonic.
 */
typedef LogCabin::Core::Time::SteadyClock Clock;

/**
 * Some point in time relative to the Clock's epoch.
 */
typedef Clock::time_point TimePoint;

typedef Core::Mutex Mutex;

/**
 * A base class for known servers in the cluster, including this process (see
 * LocalServer) and others (see Peer). This tracks various bits of state for
 * each server, which is used when we are a candidate or leader. This class
 * does not do any internal locking; it should be accessed only while holding
 * the RaftConsensus lock.
 */
class Server {
  public:
    /**
     * Constructor.
     */
    explicit Server(uint64_t serverId);
    /**
     * Destructor.
     */
    virtual ~Server();
    /**
     * Begin requesting the Server's vote in the current election. Return
     * immediately. The condition variable in RaftConsensus will be notified
     * separately.
     */
    virtual void beginRequestVote() = 0;
    /**
     * Begin replicating to the Server in the current term. Return
     * immediately. The condition variable in RaftConsensus will be notified
     * separately.
     */
    virtual void beginLeadership() = 0;
    /**
     * Inform any threads belonging to this Server to exit. Return immediately.
     * The condition variable in RaftConsensus will be notified separately.
     */
    virtual void exit() = 0;
    /**
     * Return the latest time this Server acknowledged our current term.
     */
    virtual uint64_t getLastAckEpoch() const = 0;
    /**
     * Return the largest entry ID for which this Server is known to share the
     * same entries up to and including this entry with our log.
     * This is used for advancing the leader's commitIndex.
     * Monotonically increases within a term.
     *
     * \warning
     *      Only valid when we're leader.
     */
    virtual uint64_t getMatchIndex() const = 0;
    /**
     * Return true if this Server has awarded us its vote for this term.
     */
    virtual bool haveVote() const = 0;
    /**
     * Cancel any outstanding RPCs to this Server.
     * The condition variable in RaftConsensus will be notified separately.
     */
    virtual void interrupt() = 0;
    /**
     * Return true once this Server is ready to be added to the cluster. This
     * means it has received enough of our log to where it is not expected to
     * cause an availability problem when added to the cluster configuration.
     * Should monotonically change from false to true.
     */
    virtual bool isCaughtUp() const = 0;
    /**
     * Make the next heartbeat RPC happen soon. Return immediately.
     * The condition variable in RaftConsensus will be notified separately.
     */
    virtual void scheduleHeartbeat() = 0;
    /**
     * Write this Server's state into the given structure. Used for
     * diagnostics.
     */
    virtual void
    updatePeerStats(Protocol::ServerStats::Raft::Peer& peerStats,
                    Core::Time::SteadyTimeConverter& time) const = 0;

    /**
     * Print out a Server for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os, const Server& server);

    /**
     * Virtual method for operator<<.
     */
    virtual std::ostream& dumpToStream(std::ostream& os) const = 0;

    /**
     * The ID of this server.
     */
    const uint64_t serverId;
    /**
     * The network addresses at which this server may be available
     * (comma-delimited)
     */
    std::string addresses;

    /**
     * If true, minStateMachineVersion and maxStateMachineVersion are set
     * (although they may be stale).
     */
    bool haveStateMachineSupportedVersions;
    /**
     * If haveStateMachineSupportedVersions is true, the smallest version of
     * the state machine commands/behavior that the server can support.
     */
    uint16_t minStateMachineVersion;
    /**
     * If haveStateMachineSupportedVersions is true, the largest version of
     * the state machine commands/behavior that the server can support.
     */
    uint16_t maxStateMachineVersion;

    /**
     * Used internally by Configuration for garbage collection.
     */
    bool gcFlag;
};


/**
 * A type of Server for the local process. There will only be one instance of
 * this class. Most of these methods don't do much, but they are needed to
 * satisfy the Server interface.
 */
class LocalServer : public Server {
  public:
    LocalServer(uint64_t serverId, RaftConsensus& consensus);
    ~LocalServer();
    void exit();
    void beginRequestVote();
    void beginLeadership();
    uint64_t getMatchIndex() const;
    bool haveVote() const;
    uint64_t getLastAckEpoch() const;
    void interrupt();
    bool isCaughtUp() const;
    void scheduleHeartbeat();
    std::ostream& dumpToStream(std::ostream& os) const;
    void updatePeerStats(Protocol::ServerStats::Raft::Peer& peerStats,
                         Core::Time::SteadyTimeConverter& time) const;
    RaftConsensus& consensus;
    /**
     * The index of the last log entry that has been flushed to disk.
     * Valid for leaders only. Returned by getMatchIndex() and used to
     * advance the leader's commitIndex.
     */
    uint64_t lastSyncedIndex;
};

/**
 * Represents another server in the cluster. One of these exists for each other
 * server. In addition to tracking state for each other server, this class
 * provides a thread that executes RaftConsensus::peerThreadMain().
 *
 * This class has no internal locking: in general, the RaftConsensus lock
 * should be held when accessing this class, but there are some exceptions
 * noted below.
 */
class Peer : public Server {
  public:
    /**
     * Constructor.
     */
    Peer(uint64_t serverId, RaftConsensus& consensus);

    /**
     * Destructor.
     */
    ~Peer();

    // Methods implemented from Server interface.
    void beginRequestVote();
    void beginLeadership();
    void exit();
    uint64_t getLastAckEpoch() const;
    uint64_t getMatchIndex() const;
    bool haveVote() const;
    bool isCaughtUp() const;
    void interrupt();
    void scheduleHeartbeat();

    /**
     * Returned by callRPC().
     */
    enum class CallStatus {
        /**
         * The RPC succeeded and the response was filled in.
         */
        OK,
        /**
         * No reply was received from the server. Maybe the connection was
         * dropped; maybe the RPC was canceled.
         */
        FAILED,
        /**
         * The server does not support this RPC or didn't like the arguments
         * given.
         */
        INVALID_REQUEST,
    };

    /**
     * Execute a remote procedure call on the server's RaftService. As this
     * operation might take a while, it should be called without RaftConsensus
     * lock.
     * \param[in] opCode
     *      The RPC opcode to execute (see Protocol::Raft::OpCode).
     * \param[in] request
     *      The request that was received from the other server.
     * \param[out] response
     *      Where the reply should be placed, if status is OK.
     * \param[in] lockGuard
     *      The Raft lock, which is released internally to allow for I/O
     *      concurrency.
     * \return
     *      See CallStatus.
     */
    CallStatus
    callRPC(Protocol::Raft::OpCode opCode,
            const google::protobuf::Message& request,
            google::protobuf::Message& response,
            std::unique_lock<Mutex>& lockGuard);

    /**
     * Launch this Peer's thread, which should run
     * RaftConsensus::peerThreadMain.
     * \param self
     *      A shared_ptr to this object, which the detached thread uses to make
     *      sure this object doesn't go away.
     */
    void startThread(std::shared_ptr<Peer> self);
    std::ostream& dumpToStream(std::ostream& os) const;
    void updatePeerStats(Protocol::ServerStats::Raft::Peer& peerStats,
                         Core::Time::SteadyTimeConverter& time) const;

  private:

    /**
     * Get the current session for this server. (This is cached in the #session
     * member for efficiency.) As this operation might take a while, it should
     * be called without RaftConsensus lock.
     */
    std::shared_ptr<RPC::ClientSession>
    getSession(std::unique_lock<Mutex>& lockGuard);

  public:

    /**
     * Used in startThread.
     * TODO(ongaro): reconsider
     */
    RaftConsensus& consensus;

    /**
     * A reference to the server's event loop, needed to construct new
     * sessions.
     */
    Event::Loop& eventLoop;

    /**
     * Set to true when thread should exit.
     */
    bool exiting;

    /**
     * Set to true if the server has responded to our RequestVote request in
     * the current term, false otherwise.
     */
    bool requestVoteDone;

    /**
     * See #haveVote().
     */
    bool haveVote_;

    /**
     * Indicates that the leader and the follower aren't necessarily
     * synchronized. The leader should not send large amounts of data (with
     * many log entries or large chunks of a snapshot file) to the follower
     * while this flag is true. For example, the follower might have been
     * disconnected, or the leader might not know where the follower's log
     * diverges from its own. It's better to sync up using small RPCs like
     * heartbeats, then begin/resume sending bulk data after receiving an
     * acknowledgment.
     *
     * Only used when leader.
     */
    bool suppressBulkData;

    /**
     * The index of the next entry to send to the follower. Only used when
     * leader. Minimum value of 1.
     */
    uint64_t nextIndex;

    /**
     * See #getMatchIndex().
     */
    uint64_t matchIndex;

    /**
     * See #getLastAckEpoch().
     */
    uint64_t lastAckEpoch;

    /**
     * When the next heartbeat should be sent to the follower.
     * Only valid while we're leader. The leader sends heartbeats periodically
     * if it has no new data to send, to stop the follower from starting a new
     * election.
     * \invariant
     *      This is never more than HEARTBEAT_PERIOD in the future, since
     *      new leaders don't reset it.
     */
    TimePoint nextHeartbeatTime;

    /**
     * The minimum time at which the next RPC should be sent.
     * Only valid while we're a candidate or leader. This is set when an RPC
     * fails so as to not overwhelm the network with retries (some RPCs fail
     * without timing out, for example if the remote kernel rejects the
     * connection).
     */
    TimePoint backoffUntil;

    /**
     * Counts RPC failures to issue fewer warnings.
     * Accessed only from callRPC() without holding the lock.
     */
    uint64_t rpcFailuresSinceLastWarning;

    // Used for isCaughtUp. TODO(ongaro): doc precisely
    uint64_t lastCatchUpIterationMs;
    TimePoint thisCatchUpIterationStart;
    uint64_t thisCatchUpIterationGoalId;

    /**
     * See #isCaughtUp().
     */
    bool isCaughtUp_;

    /**
     * A snapshot file to be sent to the follower, or NULL.
     * TODO(ongaro): It'd be better to destroy this as soon as this server
     * steps down, but peers don't have a hook for that right now.
     */
    std::unique_ptr<Storage::FilesystemUtil::FileContents> snapshotFile;
    /**
     * The number of bytes of 'snapshotFile' that have been acknowledged by the
     * follower already. Send starting here next time.
     */
    uint64_t snapshotFileOffset;
    /**
     * The last log index that 'snapshotFile' corresponds to. This is used to
     * set the follower's #nextIndex accordingly after we're done sending it
     * the snapshot.
     */
    uint64_t lastSnapshotIndex;

  private:

    /**
     * Caches the result of getSession().
     */
    std::shared_ptr<RPC::ClientSession> session;

    /**
     * callRPC() places its RPC here so that interrupt() may cancel it.
     * Setting this member and canceling the RPC must be done while holding the
     * Raft lock; waiting on the RPC is done without holding that lock.
     */
    RPC::ClientRPC rpc;

    // Peer is not copyable.
    Peer(const Peer&) = delete;
    Peer& operator=(const Peer&) = delete;
};

/**
 * A configuration defines the servers that are part of the cluster. This class
 * does not do any internal locking; it should be accessed only while holding
 * the RaftConsensus lock.
 */
class Configuration {
  public:
    typedef std::shared_ptr<Server> ServerRef;
    typedef std::function<bool(Server&)> Predicate;
    typedef std::function<uint64_t(Server&)> GetValue;
    typedef std::function<void(Server&)> SideEffect;

  private:
    /**
     * A list of servers in which a simple majority constitutes a quorum.
     */
    struct SimpleConfiguration {
        SimpleConfiguration();
        ~SimpleConfiguration();
        bool all(const Predicate& predicate) const;
        bool contains(ServerRef server) const;
        void forEach(const SideEffect& sideEffect);
        uint64_t min(const GetValue& getValue) const;
        bool quorumAll(const Predicate& predicate) const;
        uint64_t quorumMin(const GetValue& getValue) const;
        std::vector<ServerRef> servers;
    };

  public:
    /**
     * See #state.
     */
    enum class State {
        /**
         * The configuration specifies no servers. Servers that are new to the
         * cluster and have empty logs start in this state.
         */
        BLANK,
        /**
         * The configuration specifies a single list of servers: a quorum
         * requires any majority of this list.
         */
        STABLE,
        /**
         * The configuration specifies two lists of servers: a quorum requires
         * any majority of the first list, but the servers in the second list
         * also receive log entries.
         */
        STAGING,
        /**
         * The configuration specifies two lists of servers: a quorum requires
         * any majority of the first list and any majority of the second.
         */
        TRANSITIONAL,
    };

    /**
     * Constructor.
     */
    Configuration(uint64_t serverId, RaftConsensus& consensus);

    /**
     * Destructor.
     */
    ~Configuration();

    /**
     * Apply a function to every known server, including the local, old, new,
     * and staging servers. The function will only be called once for each
     * server, even if a server exists in more than one of these categories.
     */
    void forEach(const SideEffect& sideEffect);

    /**
     * Return true if the given server may be part of a quorum, false
     * otherwise.
     */
    bool hasVote(ServerRef server) const;

    /**
     * Lookup the network addresses for a particular server
     * (comma-delimited).
     * Returns empty string if not found.
     */
    std::string lookupAddress(uint64_t serverId) const;

    /**
     * Return true if there exists a quorum for which every server satisfies
     * the predicate, false otherwise.
     */
    bool quorumAll(const Predicate& predicate) const;

    /**
     * Return the smallest value of any server in the quorum of servers that
     * have the largest values.
     * \return
     *      Largest value for which every server in a quorum has a value
     *      greater than or equal to this one. 0 if the configuration is BLANK.
     */
    uint64_t quorumMin(const GetValue& getValue) const;

    /**
     * Remove the staging servers, if any. Return to the configuration state
     * prior to a preceding call to setStagingServers.
     */
    void resetStagingServers();

    /**
     * Set the state of this object as if it had just been constructed.
     */
    void reset();

    /**
     * Set the configuration. Any existing staging servers are dropped.
     * \param newId
     *      The log entry ID of the configuration.
     * \param newDescription
     *      The IDs and addresses of the servers in the configuration. If any
     *      newServers are listed in the description, it is considered
     *      TRANSITIONAL; otherwise, it is STABLE.
     */
    void setConfiguration(
            uint64_t newId,
            const Protocol::Raft::Configuration& newDescription);

    /**
     * Add servers that are to mirror the log but that may not have a vote
     * (listeners). This can only be called on a STABLE configuration and makes
     * it STAGING.
     * TODO(ongaro): that might be a sign of a poor interface. descriptions
     * should probably have three sets, as john said.
     */
    void setStagingServers(
            const Protocol::Raft::SimpleConfiguration& stagingServers);

    /**
     * Return true if every server in the staging set satisfies the predicate,
     * false otherwise.
     */
    bool stagingAll(const Predicate& predicate) const;

    /**
     * Return the smallest value of any server in the staging set.
     * \return
     *      Minimum value on any server in the staging set, or 0 if the staging
     *      set is empty.
     */
    uint64_t stagingMin(const GetValue& getValue) const;

    /**
     * Write the configuration servers' state into the given structure. Used
     * for diagnostics.
     */
    void updateServerStats(Protocol::ServerStats& serverStats,
                           Core::Time::SteadyTimeConverter& time) const;

    /**
     * Print out a State for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os, State state);

    /**
     * Print out a Configuration for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os,
                                    const Configuration& configuration);

  private:
    /**
     * If no server by the given ID is known, construct a new one.
     * \return
     *      Return the existing or new server.
     * TODO(ongaro): this name and signature is misleading
     */
    ServerRef getServer(uint64_t newServerId);

    /**
     * Used for constructing Server instances.
     */
    RaftConsensus& consensus;

    /**
     * A map from server ID to Server of every server, including the local,
     * previous, new, and staging servers.
     */
    std::unordered_map<uint64_t, ServerRef> knownServers;

  public:
    /**
     * This server.
     */
    std::shared_ptr<LocalServer> localServer;

    /**
     * Specifies the meaning of #oldServers and #newServers.
     */
    State state;

    /**
     * The ID of the current configuration. This is the same as the entry ID in
     * which this configuration's description is written to the log.
     */
    uint64_t id;

    /**
     * A description of the current configuration.
     */
    Protocol::Raft::Configuration description;

  private:
    /**
     * A majority of these servers are necessary for a quorum under 
     * STABLE, STAGING, and TRANSITIONAL configurations. (Under TRANSITIONAL, a
     * majority of newServers is also needed.)
     */
    SimpleConfiguration oldServers;

    /**
     * A majority of these servers are necessary for a quorum under
     * TRANSITIONAL configurations. Under STAGING configurations, these servers
     * receive log entries but do not participate in elections.
     */
    SimpleConfiguration newServers;

    friend class Invariants;
};

/**
 * Ensures the current configuration reflects the latest state of the log and
 * snapshot.
 */
class ConfigurationManager {
  public:
    /**
     * Constructor.
     * \param configuration
     *      The configuration that this object is in charge of setting.
     */
    explicit ConfigurationManager(Configuration& configuration);

    /**
     * Destructor.
     */
    ~ConfigurationManager();

    /**
     * Called when a new configuration is added to the log.
     * \param index
     *      The log index of this configuration (equivalently, its ID).
     * \param description
     *      The serializable representation of the configuration.
     */
    void add(uint64_t index,
             const Protocol::Raft::Configuration& description);
    /**
     * Called when a log prefix is truncated (after saving a snapshot that
     * covers this prefix).
     * \param firstIndexKept
     *      The log entries in range [1, firstIndexKept) are being discarded.
     */
    void truncatePrefix(uint64_t firstIndexKept);
    /**
     * Called when a log suffix is truncated (when the leader doesn't agree
     * with this server's log).
     * \param lastIndexKept
     *      The log entries in range (lastIndexKept, infinity) are being
     *      discarded.
     */
    void truncateSuffix(uint64_t lastIndexKept);
    /**
     * Called when a new snapshot is saved.
     * Only the latest such configuration is kept.
     * \param index
     *      The log index of this configuration (equivalently, its ID).
     * \param description
     *      The serializable representation of the configuration.
     */
    void setSnapshot(uint64_t index,
                     const Protocol::Raft::Configuration& description);

    /**
     * Return the configuration as of a particular log index.
     * This is useful when taking snapshots.
     * \param lastIncludedIndex
     *      Configurations greater than this index will be ignored.
     * \return
     *      The index and description of the configuration with the largest
     *      index in the range [1, lastIncludedIndex].
     */
    std::pair<uint64_t, Protocol::Raft::Configuration>
    getLatestConfigurationAsOf(uint64_t lastIncludedIndex) const;

  private:

    /**
     * Helper function called after changing this object's state.
     * - Make sure the snapshot configuration is in the descriptions map.
     * - Set configuration to the configuration with the largest index in the
     *   descriptions map, or reset it the map is empty.
     */
    void restoreInvariants();

    /**
     * Defines the servers that are part of the cluster. See Configuration.
     */
    Configuration& configuration;

    /**
     * This contains all the cluster configurations found in the log, plus one
     * additional configuration from the latest snapshot.
     *
     * It is used to find the right configuration when taking a snapshot and
     * truncating the end of the log. It must be kept consistent with the log
     * when it is loaded, as the log grows, as it gets truncated from the
     * beginning for snapshots, and as it gets truncated from the end upon
     * conflicts with the leader.
     *
     * The key is the entry ID where the configuration belongs in the log; the
     * value is the serializable form of the configuration.
     */
    std::map<uint64_t, Protocol::Raft::Configuration> descriptions;

    /**
     * This reflects the configuration found in this server's latest snapshot,
     * or {0, {}} if this server has no snapshot.
     */
    std::pair<uint64_t, Protocol::Raft::Configuration> snapshot;

    friend class Invariants;
};

/**
 * This is the rough equivalent of a SteadyClock that can be shared across the
 * network with other Raft servers. The cluster time approximately tracks how
 * long the cluster has been available with a working leader.
 *
 * Cluster time is measured in nanoseconds and progresses at about the same
 * rate as a normal clock when the cluster is operational. While there's a
 * stable leader, the nanoseconds increase according to that leader's
 * SteadyClock. When a new leader takes over, it starts ticking from cluster
 * time value it finds in its last entry/snapshot, so some cluster time may be
 * unaccounted for between the last leader replicating its final entry and then
 * losing leadership.
 *
 * The StateMachine uses cluster time to expire client sessions. Cluster times
 * in committed log entries monotonically increase, so the state machine will
 * see cluster times monotonically increase.
 *
 * Before cluster time, client sessions were expired based on the SystemClock.
 * That meant that if the SystemClock jumped forwards drastically, all clients
 * would expire. That's undesirable, so cluster time was introduced in #90
 * ("make client session timeout use monotonic clock") to address this.
 */
class ClusterClock {
  public:
    /// Default constructor.
    ClusterClock();

    /**
     * Reset to the given cluster time, assuming it's the current time right
     * now. This is used, for example, when a follower gets a new log entry
     * (which includes a cluster time) from the leader.
     */
    void newEpoch(uint64_t clusterTime);

    /**
     * Called by leaders to generate a new cluster time for a new log entry.
     * This is equivalent to the following but slightly more efficient:
     *   uint64_t now = c.interpolate();
     *   c.newEpoch(now);
     *   return now;
     */
    uint64_t leaderStamp();

    /**
     * Return the best approximation of the current cluster time, assuming
     * there's been a leader all along.
     */
    uint64_t interpolate() const;

    /**
     * Invariant: this is equal to the cluster time in:
     * - the last log entry, if any, or
     * - the last snapshot, if any, or
     * - 0.
     */
    uint64_t clusterTimeAtEpoch;

    /**
     * The local SteadyClock time when clusterTimeAtEpoch was set.
     */
    Core::Time::SteadyClock::time_point localTimeAtEpoch;
};

} // namespace RaftConsensusInternal

/**
 * An implementation of the Raft consensus algorithm. The algorithm is
 * described at https://raftconsensus.github.io
 * . In brief, Raft divides time into terms and elects a leader at the
 * beginning of each term. This election mechanism guarantees that the emerging
 * leader has at least all committed log entries. Once a candidate has received
 * votes from a quorum, it replicates its own log entries in order to the
 * followers. The leader is the only machine that serves client requests.
 */
class RaftConsensus {
  public:
    typedef RaftConsensusInternal::Invariants Invariants;
    typedef RaftConsensusInternal::Server Server;
    typedef RaftConsensusInternal::LocalServer LocalServer;
    typedef RaftConsensusInternal::Peer Peer;
    typedef RaftConsensusInternal::Configuration Configuration;
    typedef RaftConsensusInternal::ConfigurationManager ConfigurationManager;
    typedef RaftConsensusInternal::ClusterClock ClusterClock;
    typedef RaftConsensusInternal::Mutex Mutex;
    typedef RaftConsensusInternal::Clock Clock;
    typedef RaftConsensusInternal::TimePoint TimePoint;

    /**
     * This is returned by getNextEntry().
     */
    struct Entry {
        /// Default constructor.
        Entry();
        /// Move constructor.
        Entry(Entry&& other);
        /// Destructor.
        ~Entry();

        /**
         * The Raft log index for this entry (or the last one a snapshot
         * covers). Pass this as the lastIndex argument to the next call to
         * getNextEntry().
         */
        uint64_t index;

        /**
         * The type of the entry.
         */
        enum {
            /**
             * This is a normal entry containing a client request for the state
             * machine. The 'data' field contains that request, and
             * 'snapshotReader' is not set.
             */
            DATA,
            /**
             * This is a snapshot: the state machine should clear its state and
             * load in the snapshot. The 'data' field is not set, and the
             * 'snapshotReader' should be used to read the snapshot contents
             * from.
             */
            SNAPSHOT,
            /**
             * Some entries should be ignored by the state machine (they are
             * consumed internally by the consensus module). For client service
             * threads to know when a state machine is up-to-date, it's easiest
             * for the state machine to get empty entries back for these, and
             * simply call back into getNextEntry() again with the next ID,
             * Entries of type 'SKIP' will have neither 'data' nor
             * 'snapshotReader' set.
             */
            SKIP,
        } type;

        /**
         * The client request for entries of type 'DATA'.
         */
        Core::Buffer command;

        /**
         * A handle to the snapshot file for entries of type 'SNAPSHOT'.
         */
        std::unique_ptr<Storage::SnapshotFile::Reader> snapshotReader;

        /**
         * Cluster time when leader created entry/snapshot. This is valid for
         * entries of all types.
         */
        uint64_t clusterTime;

        // copy and assign not allowed
        Entry(const Entry&) = delete;
        Entry& operator=(const Entry&) = delete;
    };

    enum class ClientResult {
        /**
         * Request completed successfully.
         */
        SUCCESS,
        /**
         * Returned by setConfiguration() if the configuration could not be
         * set because the previous configuration was unsuitable or because the
         * new servers could not be caught up.
         */
        FAIL,
        /**
         * Returned by getConfiguration() if the configuration is not stable or
         * is not committed. The client should wait and retry later.
         */
        RETRY,
        /**
         * Cannot process the request because this server is not leader or
         * temporarily lost its leadership.
         */
        NOT_LEADER,
    };

    /**
     * Constructor.
     * \param globals
     *      Handle to LogCabin's top-level objects.
     */
    explicit RaftConsensus(Globals& globals);

    /**
     * Destructor.
     */
    ~RaftConsensus();

    /// Initialize. Must be called before any other method.
    void init();

    /// Signal the consensus module to exit (shut down threads, etc).
    void exit();

    /**
     * Initialize the log with a configuration consisting of just this server.
     * This should be called just once the very first time the very first
     * server in your cluster is started.
     * PANICs if any log entries or snapshots already exist.
     */
    void bootstrapConfiguration();

    /**
     * Get the current leader's active, committed, simple cluster
     * configuration.
     */
    ClientResult getConfiguration(
            Protocol::Raft::SimpleConfiguration& configuration,
            uint64_t& id) const;

    /**
     * Return the most recent entry ID that has been externalized by the
     * replicated log. This is used to provide non-stale reads to the state
     * machine.
     */
    std::pair<ClientResult, uint64_t> getLastCommitIndex() const;

    /**
     * Return the network address for a recent leader, if known,
     * or empty string otherwise.
     */
    std::string getLeaderHint() const;

    /**
     * This returns the entry following lastIndex in the replicated log. Some
     * entries may be used internally by the consensus module. These will have
     * Entry.hasData set to false. The reason these are exposed to the state
     * machine is that the state machine waits to be caught up to the latest
     * committed entry in the replicated log sometimes, but if that entry
     * was for internal use, it would would otherwise never reach the state
     * machine.
     * \throw Core::Util::ThreadInterruptedException
     *      Thread should exit.
     */
    Entry getNextEntry(uint64_t lastIndex) const;

    /**
     * Return statistics that may be useful in deciding when to snapshot.
     */
    SnapshotStats::SnapshotStats getSnapshotStats() const;

    /**
     * Process an AppendEntries RPC from another server. Called by RaftService.
     * \param[in] request
     *      The request that was received from the other server.
     * \param[out] response
     *      Where the reply should be placed.
     */
    void handleAppendEntries(
                const Protocol::Raft::AppendEntries::Request& request,
                Protocol::Raft::AppendEntries::Response& response);

    /**
     * Process an InstallSnapshot RPC from another server. Called by
     * RaftService.
     * \param[in] request
     *      The request that was received from the other server.
     * \param[out] response
     *      Where the reply should be placed.
     */
    void handleInstallSnapshot(
                const Protocol::Raft::InstallSnapshot::Request& request,
                Protocol::Raft::InstallSnapshot::Response& response);

    /**
     * Process a RequestVote RPC from another server. Called by RaftService.
     * \param[in] request
     *      The request that was received from the other server.
     * \param[out] response
     *      Where the reply should be placed.
     */
    void handleRequestVote(const Protocol::Raft::RequestVote::Request& request,
                           Protocol::Raft::RequestVote::Response& response);

    /**
     * Submit an operation to the replicated log.
     * \param operation
     *      If the cluster accepts this operation, then it will be added to the
     *      log and the state machine will eventually apply it.
     * \return
     *      First component is status code. If SUCCESS, second component is the
     *      log index at which the entry has been committed to the replicated
     *      log.
     */
    std::pair<ClientResult, uint64_t> replicate(const Core::Buffer& operation);

    /**
     * Change the cluster's configuration.
     * Returns successfully once operation completed and old servers are no
     * longer needed.
     * \return
     *      NOT_LEADER, or other code with response filled in.
     */
    ClientResult
    setConfiguration(
            const Protocol::Client::SetConfiguration::Request& request,
            Protocol::Client::SetConfiguration::Response& response);

    /**
     * Register which versions of client commands/behavior the local state
     * machine supports. Invoked just once on boot (though calling this
     * multiple times is safe). This information is used to support upgrades to
     * the running replicated state machine version, and it is transmitted to
     * other servers as needed. See #stateMachineUpdaterThreadMain.
     * \param minSupported
     *      The smallest version the local state machine can support.
     * \param maxSupported
     *      The largest version the local state machine can support.
     */
    void
    setSupportedStateMachineVersions(uint16_t minSupported,
                                     uint16_t maxSupported);

    /**
     * Start taking a snapshot. Called by the state machine when it wants to
     * take a snapshot.
     * \param lastIncludedIndex
     *      The snapshot will cover log entries in the range
     *      [1, lastIncludedIndex].
     *      lastIncludedIndex must be committed (must have been previously
     *      returned by #getNextEntry()).
     * \return
     *      A file the state machine can dump its snapshot into.
     */
    std::unique_ptr<Storage::SnapshotFile::Writer>
    beginSnapshot(uint64_t lastIncludedIndex);

    /**
     * Complete taking a snapshot for the log entries in range [1,
     * lastIncludedIndex]. Called by the state machine when it is done taking a
     * snapshot.
     * \param lastIncludedIndex
     *      The snapshot will cover log entries in the range
     *      [1, lastIncludedIndex].
     * \param writer
     *      A writer that has not yet been saved: the consensus module may
     *      have to discard the snapshot in case it's gotten a better snapshot
     *      from another server. If this snapshot is to be saved (normal case),
     *      the consensus module will call save() on it.
     */
    void snapshotDone(uint64_t lastIncludedIndex,
                      std::unique_ptr<Storage::SnapshotFile::Writer> writer);

    /**
     * Add information about the consensus state to the given structure.
     */
    void updateServerStats(Protocol::ServerStats& serverStats) const;

    /**
     * Print out the contents of this class for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os,
                                    const RaftConsensus& raft);

  private:
    /**
     * See #state.
     */
    enum class State {
        /**
         * A follower does not initiate RPCs. It becomes a candidate with
         * startNewElection() when a timeout elapses without hearing from a
         * candidate/leader. This is the initial state for servers when they
         * start up.
         */
        FOLLOWER,

        /**
         * A candidate sends RequestVote RPCs in an attempt to become a leader.
         * It steps down to be a follower if it discovers a current leader, and
         * it becomes leader if it collects votes from a quorum.
         */
        CANDIDATE,

        /**
         * A leader sends AppendEntries RPCs to replicate its log onto followers.
         * It also sends heartbeats periodically during periods of inactivity
         * to delay its followers from becoming candidates. It steps down to be
         * a follower if it discovers a server with a higher term, if it can't
         * communicate with a quorum, or if it is not part of the latest
         * committed configuration.
         */
        LEADER,
    };


    //// The following private methods MUST acquire the lock.

    /**
     * Flush log entries to stable storage in the background on leaders.
     * Once they're flushed, it tries to advance the #commitIndex.
     * This is the method that #leaderDiskThread executes.
     */
    void leaderDiskThreadMain();

    /**
     * Start new elections when it's time to do so. This is the method that
     * #timerThread executes.
     */
    void timerThreadMain();

    /**
     * Initiate RPCs to a specific server as necessary.
     * One thread for each remote server calls this method (see Peer::thread).
     */
    void peerThreadMain(std::shared_ptr<Peer> peer);

    /**
     * Append advance state machine version entries to the log as leader once
     * all servers can support a new state machine version.
     */
    void stateMachineUpdaterThreadMain();

    /**
     * Return to follower state when, as leader, this server is not able to
     * communicate with a quorum. This helps two things in cases where a quorum
     * is not available to this leader but clients can still communicate with
     * the leader. First, it returns to clients in a timely manner so that they
     * can try to find another current leader, if one exists. Second, it frees
     * up the resources associated with those client's RPCs on the server.
     * This is the method that #stepDownThread executes.
     */
    void stepDownThreadMain();


    //// The following private methods MUST NOT acquire the lock.


    /**
     * Move forward #commitIndex if possible. Called only on leaders after
     * receiving RPC responses and flushing entries to disk. If commitIndex
     * changes, this will notify #stateChanged. It will also change the
     * configuration or step down due to a configuration change when
     * appropriate.
     *
     * #commitIndex can jump by more than 1 on new leaders, since their
     * #commitIndex may be well out of date until they figure out which log
     * entries their followers have.
     *
     * \pre
     *      state is LEADER.
     */
    void advanceCommitIndex();

    /**
     * Append entries to the log, set the configuration if this contains a
     * configuration entry, and notify #stateChanged.
     */
    void append(const std::vector<const Storage::Log::Entry*>& entries);

    /**
     * Send an AppendEntries RPC to the server (either a heartbeat or containing
     * an entry to replicate).
     * \param lockGuard
     *      Used to temporarily release the lock while invoking the RPC, so as
     *      to allow for some concurrency.
     * \param peer
     *      State used in communicating with the follower, building the RPC
     *      request, and processing its result.
     */
    void appendEntries(std::unique_lock<Mutex>& lockGuard, Peer& peer);

    /**
     * Send an InstallSnapshot RPC to the server (containing part of a
     * snapshot file to replicate).
     * \param lockGuard
     *      Used to temporarily release the lock while invoking the RPC, so as
     *      to allow for some concurrency.
     * \param peer
     *      State used in communicating with the follower, building the RPC
     *      request, and processing its result.
     */
    void installSnapshot(std::unique_lock<Mutex>& lockGuard, Peer& peer);

    /**
     * Transition to being a leader. This is called when a candidate has
     * received votes from a quorum.
     */
    void becomeLeader();

    /**
     * Remove the prefix of the log that is redundant with this server's
     * snapshot.
     */
    void discardUnneededEntries();

    /**
     * Return the term corresponding to log->getLastLogIndex(). This may come
     * from the log, from the snapshot, or it may be 0.
     */
    uint64_t getLastLogTerm() const;

    /**
     * Notify the #stateChanged condition variable and cancel all current RPCs.
     * This should be called when stepping down, starting a new election,
     * becoming leader, or exiting.
     */
    void interruptAll();

    /**
     * Helper for #appendEntries() to put the right number of entries into the
     * request.
     * \param nextIndex
     *      First entry to send to the follower.
     * \param request
     *      AppendEntries request ProtoBuf in which to pack the entries.
     * \return
     *      Number of entries in the request.
     */
    uint64_t
    packEntries(uint64_t nextIndex,
                Protocol::Raft::AppendEntries::Request& request) const;

    /**
     * Try to read the latest good snapshot from disk. Loads the header of the
     * snapshot file, which is used internally by the consensus module. The
     * rest of the file reader is kept in #snapshotReader for the state machine
     * to process upon a future getNextEntry().
     *
     * If the snapshot file on disk is no good, #snapshotReader will remain
     * NULL.
     */
    void readSnapshot();

    /**
     * Append an entry to the log and wait for it to be committed.
     */
    std::pair<ClientResult, uint64_t>
    replicateEntry(Storage::Log::Entry& entry,
                   std::unique_lock<Mutex>& lockGuard);

    /**
     * Send a RequestVote RPC to the server. This is used by candidates to
     * request a server's vote and by new leaders to retrieve information about
     * the server's log.
     * \param lockGuard
     *      Used to temporarily release the lock while invoking the RPC, so as
     *      to allow for some concurrency.
     * \param peer
     *      State used in communicating with the follower, building the RPC
     *      request, and processing its result.
     */
    void requestVote(std::unique_lock<Mutex>& lockGuard, Peer& peer);

    /**
     * Dumps serverId, currentTerm, state, leaderId, and votedFor to the debug
     * log. This is intended to be easy to grep and parse.
     */
    void printElectionState() const;

    /**
     * Set the timer to start a new election and notify #stateChanged.
     * The timer is set for ELECTION_TIMEOUT plus some random jitter from
     * now.
     */
    void setElectionTimer();

    /**
     * Transitions to being a candidate from being a follower or candidate.
     * This is called when a timeout elapses. If the configuration is blank, it
     * does nothing. Moreover, if this server forms a quorum (it is the only
     * server in the configuration), this will immediately transition to
     * leader.
     */
    void startNewElection();

    /**
     * Transition to being a follower. This is called when we
     * receive an RPC request with newer term, receive an RPC response
     * indicating our term is stale, or discover a current leader while a
     * candidate. In this last case, newTerm will be the same as currentTerm.
     * This will call setElectionTimer for you if no election timer is
     * currently set.
     */
    void stepDown(uint64_t newTerm);

    /**
     * Persist critical state, such as the term and the vote, to stable
     * storage.
     */
    void updateLogMetadata();

    /**
     * Return true if every entry that might have already been marked committed
     * on any leader is marked committed on this leader by the time this call
     * returns.
     * This is used to provide non-stale read operations to
     * clients. It gives up after ELECTION_TIMEOUT, since stepDownThread
     * will return to the follower state after that time.
     */
    bool upToDateLeader(std::unique_lock<Mutex>& lockGuard) const;

    /**
     * Print out a ClientResult for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os,
                                    ClientResult clientResult);

    /**
     * Print out a State for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os, State state);

    /**
     * A follower waits for about this much inactivity before becoming a
     * candidate and starting a new election.
     */
    const std::chrono::nanoseconds ELECTION_TIMEOUT;

    /**
     * A leader sends RPCs at least this often, even if there is no data to
     * send.
     */
    const std::chrono::nanoseconds HEARTBEAT_PERIOD;

    /**
     * A leader will pack at most this many entries into an AppendEntries
     * request message. This helps bound processing time when entries are very
     * small in size.
     * Const except for unit tests.
     */
    uint64_t MAX_LOG_ENTRIES_PER_REQUEST;

    /**
     * A candidate or leader waits this long after an RPC fails before sending
     * another one, so as to not overwhelm the network with retries.
     */
    const std::chrono::nanoseconds RPC_FAILURE_BACKOFF;

    /**
     * How long the state machine updater thread should sleep if:
     * - The servers do not currently support a common version, or
     * - This server has not yet received version information from all other
     *   servers, or
     * - An advance state machine entry failed to commit (probably due to lost
     *   leadership).
     */
    const std::chrono::nanoseconds STATE_MACHINE_UPDATER_BACKOFF;

    /**
     * Prefer to keep RPC requests under this size.
     * Const except for unit tests.
     */
    uint64_t SOFT_RPC_SIZE_LIMIT;

  public:
    /**
     * This server's unique ID. Not available until init() is called.
     */
    uint64_t serverId;

    /**
     * The addresses that this server is listening on. Not available until
     * init() is called.
     */
    std::string serverAddresses;

  private:

    /**
     * The LogCabin daemon's top-level objects.
     */
    Globals& globals;

    /**
     * Where the files for the log and snapshots are stored.
     */
    Storage::Layout storageLayout;

    /**
     * Used to create new sessions.
     */
    Client::SessionManager sessionManager;

    /**
     * This class behaves mostly like a monitor. This protects all the state in
     * this class and almost all of the Peer class (with some
     * documented exceptions).
     */
    mutable Mutex mutex;

    /**
     * Notified when basically anything changes. Specifically, this is notified
     * when any of the following events occur:
     *  - term changes.
     *  - state changes.
     *  - log changes.
     *  - commitIndex changes.
     *  - exiting is set.
     *  - numPeerThreads is decremented.
     *  - configuration changes.
     *  - startElectionAt changes (see note under startElectionAt).
     *  - an acknowledgement from a peer is received.
     *  - a server goes from not caught up to caught up.
     *  - a heartbeat is scheduled.
     * TODO(ongaro): Should there be multiple condition variables? This one is
     * used by a lot of threads for a lot of different conditions.
     */
    mutable Core::ConditionVariable stateChanged;

    /**
     * Set to true when this class is about to be destroyed. When this is true,
     * threads must exit right away and no more RPCs should be sent or
     * processed.
     */
    bool exiting;

    /**
     * The number of Peer::thread threads that are still using this
     * RaftConsensus object. When they exit, they decrement this and notify
     * #stateChanged.
     */
    uint32_t numPeerThreads;

    /**
     * Provides all storage for this server. Keeps track of all log entries and
     * some additional metadata.
     *
     * If you modify this, be sure to keep #configurationManager consistent.
     */
    std::unique_ptr<Storage::Log> log;

    /**
     * Flag to indicate that #leaderDiskThreadMain should flush recent log
     * writes to stable storage. This is always false for followers and
     * candidates and is only used for leaders.
     *
     * When a server steps down, it waits for all syncs to complete, that way
     * followers can assume that all of their log entries are durable when
     * replying to leaders.
     */
    bool logSyncQueued;

    /**
     * Used for stepDown() to wait on #leaderDiskThread without releasing
     * #mutex. This is true while #leaderDiskThread is writing to disk. It's
     * set to true while holding #mutex; set to false without #mutex.
     */
    std::atomic<bool> leaderDiskThreadWorking;

    /**
     * Defines the servers that are part of the cluster. See Configuration.
     */
    std::unique_ptr<Configuration> configuration;

    /**
     * Ensures that 'configuration' reflects the latest state of the log and
     * snapshot.
     */
    std::unique_ptr<ConfigurationManager> configurationManager;

    /**
     * The latest term this server has seen. This value monotonically increases
     * over time. It gets updated in stepDown(), startNewElection(), and when a
     * candidate receives a vote response with a newer term.
     * \warning
     *      After setting this value, you must call updateLogMetadata() to
     *      persist it.
     */
    uint64_t currentTerm;

    /**
     * The server's current role in the cluster (follower, candidate, or
     * leader). See #State.
     */
    State state;

    /**
     * The latest good snapshot covers entries 1 through 'lastSnapshotIndex'
     * (inclusive). It is known that these are committed. They are safe to
     * remove from the log, but it may be advantageous to keep them around for
     * a little while (to avoid shipping snapshots to straggling followers).
     * Thus, the log may or may not have some of the entries in this range.
     */
    uint64_t lastSnapshotIndex;

    /**
     * The term of the last entry covered by the latest good snapshot, or 0 if
     * we have no snapshot.
     */
    uint64_t lastSnapshotTerm;

    /**
     * The cluster time of the last entry covered by the latest good snapshot,
     * or 0 if we have no snapshot.
     */
    uint64_t lastSnapshotClusterTime;

    /**
     * The size of the latest good snapshot in bytes, or 0 if we have no
     * snapshot.
     */
    uint64_t lastSnapshotBytes;

    /**
     * If not NULL, this is a Storage::SnapshotFile::Reader that covers up through
     * lastSnapshotIndex. This is ready for the state machine to process and is
     * returned to the state machine in getNextEntry(). It's just a cache which
     * can be repopulated with readSnapshot().
     */
    mutable std::unique_ptr<Storage::SnapshotFile::Reader> snapshotReader;

    /**
     * This is used in handleInstallSnapshot when receiving a snapshot from
     * the current leader. The leader is assumed to send at most one snapshot
     * at a time, and any partial snapshots here are discarded when the term
     * changes.
     */
    std::unique_ptr<Storage::SnapshotFile::Writer> snapshotWriter;

    /**
     * The largest entry ID for which a quorum is known to have stored the same
     * entry as this server has. Entries 1 through commitIndex as stored in
     * this server's log are guaranteed to never change. This value will
     * monotonically increase over time.
     */
    uint64_t commitIndex;

    /**
     * The server ID of the leader for this term. This is used to help point
     * clients to the right server. The special value 0 means either there is
     * no leader for this term yet or this server does not know who it is yet.
     */
    uint64_t leaderId;

    /**
     * The server ID that this server voted for during this term's election, if
     * any. The special value 0 means no vote has been given out during this
     * term.
     * \warning
     *      After setting this value, you must call updateLogMetadata() to
     *      persist it.
     */
    uint64_t votedFor;

    /**
     * A logical clock used to confirm leadership and connectivity.
     */
    // TODO(ongaro): rename, explain more
    mutable uint64_t currentEpoch;

    /**
     * Tracks the passage of "cluster time". See ClusterClock.
     */
    ClusterClock clusterClock;

    /**
     * The earliest time at which #timerThread should begin a new election
     * with startNewElection().
     *
     * It is safe for increases to startElectionAt to not notify the condition
     * variable. Decreases to this value, however, must notify the condition
     * variable to make sure the timerThread gets woken in a timely manner.
     * Unfortunately, startElectionAt does not monotonically increase because
     * of the random jitter that is applied to the follower timeout, and it
     * would reduce the jitter's effectiveness for the thread to wait as long
     * as the largest startElectionAt value.
     */
    TimePoint startElectionAt;

    /**
     * The earliest time at which RequestVote messages should be processed.
     * Until this time, they are rejected, as processing them risks
     * causing the cluster leader to needlessly step down.
     * For more motivation, see the "disruptive servers" issue in membership
     * changes described in the Raft paper/thesis.
     *
     * This is set to the current time + an election timeout when a heartbeat
     * is received, and it's set to infinity for leaders (who begin processing
     * RequestVote messages again immediately when they step down).
     */
    TimePoint withholdVotesUntil;

    /**
     * The total number of entries ever truncated from the end of the log.
     * This happens only when a new leader tells this server to remove
     * extraneous uncommitted entries from its log.
     */
    uint64_t numEntriesTruncated;

    /**
     * The thread that executes leaderDiskThreadMain() to flush log entries to
     * stable storage in the background on leaders.
     */
    std::thread leaderDiskThread;

    /**
     * The thread that executes timerThreadMain() to begin new elections
     * after periods of inactivity.
     */
    std::thread timerThread;

    /**
     * The thread that executes stateMachineUpdaterThreadMain() to append
     * advance state machine version entries to the log on leaders.
     */
    std::thread stateMachineUpdaterThread;

    /**
     * The thread that executes stepDownThreadMain() to return to the follower
     * state if the leader becomes disconnected from a quorum of servers.
     */
    std::thread stepDownThread;

    Invariants invariants;

    friend class RaftConsensusInternal::LocalServer;
    friend class RaftConsensusInternal::Peer;
    friend class RaftConsensusInternal::Invariants;
};

} // namespace LogCabin::Server
} // namespace LogCabin

#endif /* LOGCABIN_SERVER_RAFTCONSENSUS_H */
