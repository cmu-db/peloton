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

#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Server/RaftConsensus.h"

namespace LogCabin {
namespace Server {
namespace RaftConsensusInternal {

#define expect(expr) do { \
    if (!(expr)) { \
        WARNING("`%s' is false", #expr); \
        ++errors; \
    } \
} while (0)

struct Invariants::ConsensusSnapshot {
    explicit ConsensusSnapshot(const RaftConsensus& consensus)
        : stateChangedCount(consensus.stateChanged.notificationCount)
        , exiting(consensus.exiting)
        , numPeerThreads(consensus.numPeerThreads)
        , lastLogIndex(consensus.log->getLastLogIndex())
        , lastLogTerm(0)
        , configurationId(consensus.configuration->id)
        , configurationState(consensus.configuration->state)
        , currentTerm(consensus.currentTerm)
        , state(consensus.state)
        , commitIndex(consensus.commitIndex)
        , leaderId(consensus.leaderId)
        , votedFor(consensus.votedFor)
        , currentEpoch(consensus.currentEpoch)
        , startElectionAt(consensus.startElectionAt)
    {
        if (consensus.log->getLastLogIndex() >=
            consensus.log->getLogStartIndex()) {
            lastLogTerm = consensus.log->getEntry(
                                    consensus.log->getLastLogIndex()).term();
        }

    }

    uint64_t stateChangedCount;
    bool exiting;
    uint32_t numPeerThreads;
    uint64_t lastLogIndex;
    uint64_t lastLogTerm;
    uint64_t configurationId;
    Configuration::State configurationState;
    uint64_t currentTerm;
    RaftConsensus::State state;
    uint64_t commitIndex;
    uint64_t leaderId;
    uint64_t votedFor;
    uint64_t currentEpoch;
    TimePoint startElectionAt;
};


Invariants::Invariants(RaftConsensus& consensus)
    : consensus(consensus)
    , errors(0)
    , previous()
{
}

Invariants::~Invariants()
{
}

void
Invariants::checkAll()
{
    checkBasic();
    checkDelta();
    checkPeerBasic();
    checkPeerDelta();
}

void
Invariants::checkBasic()
{
    // Log terms and cluster times monotonically increase
    uint64_t lastTerm = 0;
    uint64_t lastClusterTime = 0;
    for (uint64_t index = consensus.log->getLogStartIndex();
         index <= consensus.log->getLastLogIndex();
         ++index) {
        const Storage::Log::Entry& entry = consensus.log->getEntry(index);
        expect(entry.term() >= lastTerm);
        expect(entry.cluster_time() >= lastClusterTime);
        lastTerm = entry.term();
        lastClusterTime = entry.cluster_time();
    }
    // The terms in the log do not exceed currentTerm
    expect(lastTerm <= consensus.currentTerm);

    if (consensus.log->getLogStartIndex() <=
        consensus.log->getLastLogIndex()) {
        expect(lastClusterTime ==
               consensus.clusterClock.clusterTimeAtEpoch);
    } else {
        expect(consensus.lastSnapshotClusterTime ==
               consensus.clusterClock.clusterTimeAtEpoch);
    }

    // The current configuration should be the last one found in the log
    bool found = false;
    for (uint64_t index = consensus.log->getLastLogIndex();
         index >= consensus.log->getLogStartIndex();
         --index) {
        const Storage::Log::Entry& entry = consensus.log->getEntry(index);
        if (entry.type() == Protocol::Raft::EntryType::CONFIGURATION) {
            expect(consensus.configuration->id == index);
            expect(consensus.configuration->state !=
                   Configuration::State::BLANK);
            found = true;
            break;
        }
    }
    if (!found) {
        if (consensus.log->getLogStartIndex() == 1) {
            expect(consensus.configuration->id == 0);
            expect(consensus.configuration->state ==
                   Configuration::State::BLANK);
        } else {
            expect(consensus.configuration->id <= consensus.lastSnapshotIndex);
        }
    }

    // Every configuration present in the log should also be present in the
    // configurationDescriptions map.
    for (uint64_t index = consensus.log->getLogStartIndex();
         index <= consensus.log->getLastLogIndex();
         ++index) {
        const Storage::Log::Entry& entry = consensus.log->getEntry(index);
        if (entry.type() == Protocol::Raft::EntryType::CONFIGURATION) {
            auto it = consensus.configurationManager->
                                        descriptions.find(index);
            expect(it != consensus.configurationManager->descriptions.end());
            if (it != consensus.configurationManager->descriptions.end())
                expect(it->second == entry.configuration());
        }
    }
    // The configuration descriptions map shouldn't have anything past the
    // snapshot and the log.
    expect(consensus.configurationManager->descriptions.upper_bound(
                std::max(consensus.log->getLastLogIndex(),
                         consensus.lastSnapshotIndex)) ==
           consensus.configurationManager->descriptions.end());

    // Servers with blank configurations should remain passive. Since the first
    // entry in every log is a configuration, they should also have empty logs.
    if (consensus.configuration->state == Configuration::State::BLANK) {
        expect(consensus.state == RaftConsensus::State::FOLLOWER);
        expect(consensus.log->getLastLogIndex() == 0);
    }

    // The last snapshot covers a committed range.
    expect(consensus.commitIndex >= consensus.lastSnapshotIndex);

    // The commitIndex doesn't exceed the length of the log/snapshot.
    expect(consensus.commitIndex <= consensus.log->getLastLogIndex());

    // The last log index points at least through the end of the last snapshot.
    expect(consensus.log->getLastLogIndex() >= consensus.lastSnapshotIndex);

    // lastLogIndex is either just below the log start (for empty logs) or
    // larger (for non-empty logs)
    assert(consensus.log->getLastLogIndex() >=
           consensus.log->getLogStartIndex() - 1);

    // advanceCommitIndex is called everywhere it needs to be.
    if (consensus.state == RaftConsensus::State::LEADER) {
        uint64_t majorityEntry =
            consensus.configuration->quorumMin(&Server::getMatchIndex);
        expect(consensus.commitIndex >= majorityEntry ||
               majorityEntry < consensus.log->getLogStartIndex() ||
               consensus.log->getEntry(majorityEntry).term() !=
                    consensus.currentTerm);
    }

    // A leader always points its leaderId at itself.
    if (consensus.state == RaftConsensus::State::LEADER)
        expect(consensus.leaderId == consensus.serverId);

    // A leader always voted for itself. (Candidates can vote for others when
    // they abort an election.)
    if (consensus.state == RaftConsensus::State::LEADER) {
        expect(consensus.votedFor == consensus.serverId);
    }

    // A follower and candidate always has a timer set; a leader has it at
    // TimePoint::max().
    if (consensus.state == RaftConsensus::State::LEADER) {
        expect(consensus.startElectionAt == TimePoint::max());
    } else {
        expect(consensus.startElectionAt > TimePoint::min());
        expect(consensus.startElectionAt <=
               Clock::now() + consensus.ELECTION_TIMEOUT * 2);
    }

    // Log metadata is updated when the term or vote changes.
    expect(consensus.log->metadata.current_term() == consensus.currentTerm);
    expect(consensus.log->metadata.voted_for() == consensus.votedFor);
}

void
Invariants::checkDelta()
{
    if (!previous) {
        previous.reset(new ConsensusSnapshot(consensus));
        return;
    }
    std::unique_ptr<ConsensusSnapshot> current(
                                        new ConsensusSnapshot(consensus));
    // Within a term, ...
    if (previous->currentTerm == current->currentTerm) {
        // the leader is set at most once.
        if (previous->leaderId != 0)
            expect(previous->leaderId == current->leaderId);
        // the vote is set at most once.
        if (previous->votedFor != 0)
            expect(previous->votedFor == current->votedFor);
        // a leader stays a leader.
        if (previous->state == RaftConsensus::State::LEADER)
            expect(current->state == RaftConsensus::State::LEADER);
    }

    // Once exiting is set, it doesn't get unset.
    if (previous->exiting)
        expect(current->exiting);

    // These variables monotonically increase.
    expect(previous->currentTerm <= current->currentTerm);
    expect(previous->commitIndex <= current->commitIndex);
    expect(previous->currentEpoch <= current->currentEpoch);

    // Change requires condition variable notification:
    if (previous->stateChangedCount == current->stateChangedCount) {
        expect(previous->currentTerm == current->currentTerm);
        expect(previous->state == current->state);
        expect(previous->lastLogIndex == current->lastLogIndex);
        expect(previous->lastLogTerm == current->lastLogTerm);
        expect(previous->commitIndex == current->commitIndex);
        expect(previous->exiting == current->exiting);
        expect(previous->numPeerThreads <= current->numPeerThreads);
        expect(previous->configurationId == current->configurationId);
        expect(previous->configurationState == current->configurationState);
        expect(previous->startElectionAt == current->startElectionAt);
     // TODO(ongaro):
     // an acknowledgement from a peer is received.
     // a server goes from not caught up to caught up.
    }

    previous = std::move(current);
}

void
Invariants::checkPeerBasic()
{
    for (auto it = consensus.configuration->knownServers.begin();
         it != consensus.configuration->knownServers.end();
         ++it) {
        Peer* peer = dynamic_cast<Peer*>(it->second.get()); // NOLINT
        if (peer == NULL)
            continue;
        if (consensus.exiting)
            expect(peer->exiting);
        if (!peer->requestVoteDone) {
            expect(!peer->haveVote_);
        }
        expect(peer->matchIndex <= consensus.log->getLastLogIndex());
        expect(peer->lastAckEpoch <= consensus.currentEpoch);
        expect(peer->nextHeartbeatTime <=
               Clock::now() + consensus.HEARTBEAT_PERIOD);
        expect(peer->backoffUntil <=
               Clock::now() + consensus.RPC_FAILURE_BACKOFF);

        // TODO(ongaro): anything about catchup?
    }
}

void
Invariants::checkPeerDelta()
{
    // TODO(ongaro): add checks
}

} // namespace RaftConsensusInternal

} // namespace LogCabin::Server
} // namespace LogCabin
