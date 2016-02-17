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

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "build/Protocol/Client.pb.h"
#include "build/Server/SnapshotStateMachine.pb.h"
#include "Core/ConditionVariable.h"
#include "Core/Config.h"
#include "Core/Mutex.h"
#include "Core/Time.h"
#include "Tree/Tree.h"

#ifndef LOGCABIN_SERVER_STATEMACHINE_H
#define LOGCABIN_SERVER_STATEMACHINE_H

namespace LogCabin {
namespace Server {

// forward declaration
class Globals;
class RaftConsensus;

/**
 * Interprets and executes operations that have been committed into the Raft
 * log.
 *
 * Version history:
 * - Version 1 of the State Machine shipped with LogCabin v1.0.0.
 * - Version 2 added the CloseSession command, which clients can use when they
 *   gracefully shut down.
 */
class StateMachine {
  public:
    typedef Protocol::Client::StateMachineCommand Command;
    typedef Protocol::Client::StateMachineQuery Query;

    enum {
        /**
         * This state machine code can behave like all versions between
         * MIN_SUPPORTED_VERSION and MAX_SUPPORTED_VERSION, inclusive.
         */
        MIN_SUPPORTED_VERSION = 1,
        /**
         * This state machine code can behave like all versions between
         * MIN_SUPPORTED_VERSION and MAX_SUPPORTED_VERSION, inclusive.
         */
        MAX_SUPPORTED_VERSION = 2,
    };


    StateMachine(std::shared_ptr<RaftConsensus> consensus,
                 Core::Config& config,
                 Globals& globals);
    ~StateMachine();

    /**
     * Called by ClientService to execute read-only queries on the state
     * machine.
     * \warning
     *      Be sure to wait() first!
     */
    bool query(const Query::Request& request,
               Query::Response& response) const;

    /**
     * Add information about the state machine state to the given structure.
     */
    void updateServerStats(Protocol::ServerStats& serverStats) const;

    /**
     * Return once the state machine has applied at least the given entry.
     */
    void wait(uint64_t index) const;

    /**
     * Called by ClientService to get a response for a read-write command on
     * the state machine.
     * \param logIndex
     *      The index in the log where the command was committed.
     * \param command
     *      The request.
     * \param[out] response
     *      If the return value is true, the response will be filled in here.
     *      Otherwise, this will be unmodified.
     */
    bool waitForResponse(uint64_t logIndex,
                         const Command::Request& command,
                         Command::Response& response) const;

    /**
     * Return true if the server is currently taking a snapshot and false
     * otherwise.
     */
    bool isTakingSnapshot() const;

    /**
     * If the state machine is not taking a snapshot, this starts one. This
     * method will return after the snapshot has been started (it may have
     * already completed).
     */
    void startTakingSnapshot();

    /**
     * If the server is currently taking a snapshot, abort it. This method will
     * return after the existing snapshot has been stopped (it's possible that
     * another snapshot will have already started).
     */
    void stopTakingSnapshot();

    /**
     * Return the time for which the state machine will not take any automated
     * snapshots.
     * \return
     *      Zero or positive duration.
     */
    std::chrono::nanoseconds getInhibit() const;

    /**
     * Disable automated snapshots for the given duration.
     * \param duration
     *      If zero or negative, immediately enables automated snapshotting.
     *      If positive, disables automated snapshotting for the given duration
     *      (or until a later call to #setInhibit()).
     * Note that this will not stop the current snapshot; the caller should
     * invoke #stopTakingSnapshot() separately if desired.
     */
    void setInhibit(std::chrono::nanoseconds duration);


  private:
    // forward declaration
    struct Session;

    /// Clock used by watchdog timer thread.
    typedef Core::Time::SteadyClock Clock;
    /// Point in time of Clock.
    typedef Clock::time_point TimePoint;

    /**
     * Invoked once per committed entry from the Raft log.
     */
    void apply(const RaftConsensus::Entry& entry);

    /**
     * Main function for thread that waits for new commands from Raft.
     */
    void applyThreadMain();

    /**
     * Return the #sessions table as a protobuf message for writing into a
     * snapshot.
     */
    void serializeSessions(SnapshotStateMachine::Header& header) const;

    /**
     * Update the session and clean up unnecessary responses.
     * \param session
     *      Affected session.
     * \param firstOutstandingRPC
     *      New value for the first outstanding RPC for a session.
     */
    void expireResponses(Session& session, uint64_t firstOutstandingRPC);

    /**
     * Remove old sessions.
     * \param clusterTime
     *      Sessions are kept if they have been modified during the last
     *      timeout period going backwards from the given time.
     */
    void expireSessions(uint64_t clusterTime);

    /**
     * Return the version of the state machine behavior as of the given log
     * index. Note that this is based on versionHistory internally, so if
     * you're changing that variable at the given index, update it first.
     */
    uint16_t getVersion(uint64_t logIndex) const;

    /**
     * If there is a current snapshot process, send it a signal and return
     * immediately.
     */
    void killSnapshotProcess(Core::HoldingMutex holdingMutex, int signum);

    /**
     * Restore the #sessions table from a snapshot.
     */
    void loadSessions(const SnapshotStateMachine::Header& header);

    /**
     * Read all of the state machine state from a snapshot file
     * (including version, sessions, and tree).
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);

    /**
     * Restore the #versionHistory table from a snapshot.
     */
    void loadVersionHistory(const SnapshotStateMachine::Header& header);

    /**
     * Return the #versionHistory table as a protobuf message for writing into
     * a snapshot.
     */
    void serializeVersionHistory(SnapshotStateMachine::Header& header) const;

    /**
     * Return true if it is time to create a new snapshot.
     * This is called by applyThread as an optimization to avoid waking up
     * snapshotThread upon applying every single entry.
     * \warning
     *      Callers should take care to honor maySnapshotAt; this method
     *      ignores it.
     */
    bool shouldTakeSnapshot(uint64_t lastIncludedIndex) const;

    /**
     * Main function for thread that calls takeSnapshot when appropriate.
     */
    void snapshotThreadMain();

    /**
     * Main function for thread that checks the progress of the child process.
     */
    void snapshotWatchdogThreadMain();

    /**
     * Called by snapshotThreadMain to actually take the snapshot.
     */
    void takeSnapshot(uint64_t lastIncludedIndex,
                      std::unique_lock<Core::Mutex>& lockGuard);

    /**
     * Called to log a debug message if appropriate when the state machine
     * encounters a query or command that is not understood by the current
     * running version.
     * \param request
     *      Problematic command/query.
     * \param reason
     *      Explains why 'request' is problematic. Should complete the sentence
     *      "This version of the state machine (%lu) " + reason, and it should
     *      not contain end punctuation.
     */
    void warnUnknownRequest(const google::protobuf::Message& request,
                            const char* reason) const;

    /**
     * Consensus module from which this state machine pulls commands and
     * snapshots.
     */
    std::shared_ptr<RaftConsensus> consensus;

    /**
     * Server-wide globals. Used to unblock signal handlers in child process.
     */
    Globals& globals;

    /**
     * Used for testing the snapshot watchdog thread. The probability that a
     * snapshotting process will deadlock on purpose before starting, as a
     * percentage.
     */
    uint64_t snapshotBlockPercentage;

    /**
     * Size in bytes of smallest log to snapshot.
     */
    uint64_t snapshotMinLogSize;

    /**
     * Maximum log size as multiple of last snapshot size until server should
     * snapshot.
     */
    uint64_t snapshotRatio;

    /**
     * After this much time has elapsed without any progress, the snapshot
     * watchdog thread will kill the snapshotting process. A special value of 0
     * disables the watchdog entirely.
     */
    std::chrono::nanoseconds snapshotWatchdogInterval;

    /**
     * The time interval after which to remove an inactive client session, in
     * nanoseconds of cluster time.
     */
    uint64_t sessionTimeoutNanos;

    /**
     * The state machine logs messages when it receives a command or query that
     * is not understood in the current running version. This controls the
     * minimum interval between such messages to prevent spamming the debug
     * log.
     */
    std::chrono::milliseconds unknownRequestMessageBackoff;

    /**
     * Protects against concurrent access for all members of this class (except
     * 'consensus', which is itself a monitor.
     */
    mutable Core::Mutex mutex;

    /**
     * Notified when lastApplied changes after some entry got applied.
     * Also notified upon exiting.
     * This is used for client threads to wait; see wait().
     */
    mutable Core::ConditionVariable entriesApplied;

    /**
     * Notified when shouldTakeSnapshot(lastApplied) becomes true.
     * Also notified upon exiting and when #maySnapshotAt changes.
     * This is used for snapshotThread to wake up only when necessary.
     */
    mutable Core::ConditionVariable snapshotSuggested;

    /**
     * Notified when a snapshot process is forked.
     * Also notified upon exiting.
     * This is used so that the watchdog thread knows to begin checking the
     * progress of the child process, and also in #startTakingSnapshot().
     */
    mutable Core::ConditionVariable snapshotStarted;

    /**
     * Notified when a snapshot process is joined.
     * Also notified upon exiting.
     * This is used so that #stopTakingSnapshot() knows when it's done.
     */
    mutable Core::ConditionVariable snapshotCompleted;

    /**
     * applyThread sets this to true to signal that the server is shutting
     * down.
     */
    bool exiting;

    /**
     * The PID of snapshotThread's child process, if any. This is used by
     * applyThread to signal exits: if applyThread is exiting, it sends SIGTERM
     * to this child process. A childPid of 0 indicates that there is no child
     * process.
     */
    pid_t childPid;

    /**
     * The index of the last log entry that this state machine has applied.
     * This variable is only written to by applyThread, so applyThread is free
     * to access this variable without holding 'mutex'. Other readers must hold
     * 'mutex'.
     */
    uint64_t lastApplied;

    /**
     * The time when warnUnknownRequest() last printed a debug message. Used to
     * prevent spamming the debug log.
     */
    mutable TimePoint lastUnknownRequestMessage;

    /**
     * Total number of commands/queries that this state machine either did not
     * understand or could not process because they were introduced in a newer
     * version.
     */
    mutable uint64_t numUnknownRequests;

    /**
     * The number of debug messages suppressed by warnUnknownRequest() since
     * lastUnknownRequestMessage. Used to prevent spamming the debug log.
     */
    mutable uint64_t numUnknownRequestsSinceLastMessage;

    /**
     * The number of times a snapshot has been started.
     * In addition to being a useful stat, the watchdog thread uses this to
     * know whether it's been watching the same snapshot or whether a new one
     * has been started, and startTakingSnapshot() waits for this to change
     * before returning.
     */
    uint64_t numSnapshotsAttempted;

    /**
     * The number of times a snapshot child process has failed to exit cleanly.
     */
    uint64_t numSnapshotsFailed;

    /**
     * The number of times a log entry was processed to advance the state
     * machine's running version, but the state machine was already at that
     * version.
     */
    uint64_t numRedundantAdvanceVersionEntries;

    /**
     * The number of times a log entry was processed to advance the state
     * machine's running version, but the state machine was already at a larger
     * version.
     */
    uint64_t numRejectedAdvanceVersionEntries;

    /**
     * The number of times a log entry was processed to successfully advance
     * the state machine's running version, where the state machine was
     * previously at a smaller version.
     */
    uint64_t numSuccessfulAdvanceVersionEntries;

    /**
     * The number of times any log entry to advance the state machine's running
     * version was processed. Should be the sum of redundant, rejected, and
     * successful counts.
     */
    uint64_t numTotalAdvanceVersionEntries;

    /**
     * Set to true when an administrator has asked the server to take a
     * snapshot; set to false once the server starts any snapshot.
     * Snapshots that are requested due to this flag are permitted to begin
     * even if automated snapshots have been inhibited with #setInhibit().
     */
    bool isSnapshotRequested;

    /**
     * The time at which the server may begin to take automated snapshots.
     * Normally this is set to some time in the past. When automated snapshots
     * are inhibited with #setInhibit(), this will be set to a future time.
     */
    TimePoint maySnapshotAt;

    /**
     * Tracks state for a particular client.
     * Used to prevent duplicate processing of duplicate RPCs.
     */
    struct Session {
        Session()
            : lastModified(0)
            , firstOutstandingRPC(0)
            , responses()
        {
        }
        /**
         * When the session was last active, measured in cluster time
         * (roughly the number of nanoseconds that the cluster has maintained a
         * leader).
         */
        uint64_t lastModified;
        /**
         * Largest firstOutstandingRPC number processed from this client.
         */
        uint64_t firstOutstandingRPC;
        /**
         * Maps from RPC numbers to responses.
         * Responses for RPCs numbered less that firstOutstandingRPC are
         * discarded from this map.
         */
        std::unordered_map<uint64_t,
                           Protocol::Client::StateMachineCommand::Response>
            responses;
    };

    /**
     * Client ID to Session map.
     */
    std::unordered_map<uint64_t, Session> sessions;

    /**
     * The hierarchical key-value store. Used in readOnlyTreeRPC and
     * readWriteTreeRPC.
     */
    Tree::Tree tree;

    /**
     * The log position when the state machine was updated to each new version.
     * First component: log index. Second component: version number.
     * Used to evolve state machine over time.
     *
     * This is used by getResponse() to determine the running version at a
     * given log index (to determine whether a command would have been
     * applied), and it's used elsewhere to determine the state machine's
     * current running version.
     *
     * Invariant: the pair (index 0, version 1) is always present.
     */
    std::map<uint64_t, uint16_t> versionHistory;

    /**
     * The file that the snapshot is being written into. Also used by to track
     * the progress of the child process for the watchdog thread.
     * This is non-empty if and only if childPid > 0.
     */
    std::unique_ptr<Storage::SnapshotFile::Writer> writer;

    /**
     * Repeatedly calls into the consensus module to get commands to process
     * and applies them.
     */
    std::thread applyThread;

    /**
     * Takes snapshots with the help of a child process.
     */
    std::thread snapshotThread;

    /**
     * Watches the child process to make sure it's writing to #writer, and
     * kills it otherwise. This is to detect any possible deadlock that might
     * occur if a thread in the parent at the time of the fork held a lock that
     * the child process then tried to access.
     * See https://github.com/logcabin/logcabin/issues/121 for more rationale.
     */
    std::thread snapshotWatchdogThread;
};

} // namespace LogCabin::Server
} // namespace LogCabin

#endif // LOGCABIN_SERVER_STATEMACHINE_H
