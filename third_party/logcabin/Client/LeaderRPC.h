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

#include <cinttypes>
#include <deque>
#include <memory>
#include <mutex>

#include "build/Protocol/Client.pb.h"
#include "Client/SessionManager.h"
#include "Core/ConditionVariable.h"
#include "RPC/Address.h"
#include "RPC/ClientRPC.h"

#ifndef LOGCABIN_CLIENT_LEADERRPC_H
#define LOGCABIN_CLIENT_LEADERRPC_H

namespace LogCabin {

// forward declaration
namespace Core {
class Config;
}

// forward declaration
namespace Event {
class Loop;
}

// forward declaration
namespace RPC {
class ClientSession;
}

namespace Client {

// forward declaration
class Backoff;

/**
 * This class is used to send RPCs from clients to the leader of the LogCabin
 * cluster. It automatically finds and connects to the leader and transparently
 * rolls over to a new leader when necessary.
 *
 * There are two implementations of this interface: LeaderRPC is probably the
 * one you're interested in. LeaderRPCMock is used for unit testing only.
 */
class LeaderRPCBase {
  public:
    /// Clock used for timeouts.
    typedef RPC::ClientRPC::Clock Clock;
    /// Type for absolute time values used for timeouts.
    typedef RPC::ClientRPC::TimePoint TimePoint;

    /**
     * RPC operation code.
     */
    typedef Protocol::Client::OpCode OpCode;

    /**
     * Return type for LeaderRPCBase::call().
     */
    enum class Status {
        /**
         * The RPC completed scucessfully.
         */
        OK,
        /**
         * The given timeout elapsed before the RPC completed.
         */
        TIMEOUT,
        /**
         * The server rejected the request, probably because it doesn't
         * support the opcode, or maybe the request arguments were invalid.
         */
        INVALID_REQUEST,
    };

    /**
     * Print out a Status for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os, const Status& server);

    /// Constructor.
    LeaderRPCBase() {}

    /// Destructor.
    virtual ~LeaderRPCBase() {}

    /**
     * Execute an RPC on the cluster leader.
     * This class guarantees that the RPC will be executed at least once.
     * \param opCode
     *      RPC operation code. The caller must guarantee that this is a valid
     *      opCode. (If the server rejects it, this will PANIC.)
     * \param request
     *      The parameters for the operation. The caller must guarantee that
     *      this is a well-formed request. (If the server rejects it, this will
     *      PANIC.)
     * \param timeout
     *      After this time has elapsed, stop waiting and return TIMEOUT.
     *      In this case, response will be left unmodified.
     * \param[out] response
     *      The response to the operation will be filled in here.
     */
    virtual Status call(OpCode opCode,
                        const google::protobuf::Message& request,
                        google::protobuf::Message& response,
                        TimePoint timeout) = 0;

    /**
     * An asynchronous version of call(). This allows multiple RPCs to be
     * executed concurrently, or canceling an RPC that is running on a separate
     * thread.
     */
    class Call {
      public:
        /**
         * Return type for LeaderRPCBase::Call::wait().
         */
        enum class Status {
            /**
             * The RPC completed scucessfully.
             */
            OK,
            /**
             * The RPC did not succeed, nor did it timeout.
             * The caller should try again.
             * TODO(ongaro): this is a bit ugly
             */
            RETRY,
            /**
             * The given timeout elapsed before the RPC completed.
             */
            TIMEOUT,
            /**
             * The server rejected the request, probably because it doesn't
             * support the opcode, or maybe the request arguments were invalid.
             */
            INVALID_REQUEST,
        };

        /**
         * Print out a Status for debugging purposes.
         */
        friend std::ostream& operator<<(std::ostream& os,
                                        const Status& server);

        /**
         * Constructor.
         */
        Call() {}
        /**
         * Destructor.
         */
        virtual ~Call() {}
        /**
         * Invoke the RPC.
         * \param opCode
         *      RPC operation code. The caller must guarantee that this is a
         *      valid opCode. (If the server rejects it, this will PANIC.)
         * \param request
         *      The parameters for the operation. The caller must guarantee
         *      that this is a well-formed request. (If the server rejects it,
         *      this will PANIC.)
         * \param timeout
         *      After this time has elapsed, stop trying to initiate the
         *      connection to the leader and use an invalid session, which will
         *      cause the RPC to fail later.
         */
        virtual void start(OpCode opCode,
                           const google::protobuf::Message& request,
                           TimePoint timeout) = 0;
        /**
         * Cancel the RPC. This may only be called after start(), but it may
         * be called safely from a separate thread.
         */
        virtual void cancel() = 0;
        /**
         * Wait for the RPC to complete.
         * \param[out] response
         *      If successful, the response to the operation will be filled in
         *      here.
         * \param timeout
         *      After this time has elapsed, stop waiting and return TIMEOUT.
         *      In this case, response will be left unmodified.
         * \return
         *      True if the RPC completed successfully, false otherwise. If
         *      this returns false, it is the callers responsibility to start
         *      over to achieve the same at-most-once semantics as #call().
         */
        virtual Status wait(google::protobuf::Message& response,
                            TimePoint timeout) = 0;
    };

    /**
     * Return a new Call object.
     */
    virtual std::unique_ptr<Call> makeCall() = 0;

    // LeaderRPCBase is not copyable
    LeaderRPCBase(const LeaderRPCBase&) = delete;
    LeaderRPCBase& operator=(const LeaderRPCBase&) = delete;
};


/**
 * This is the implementation of LeaderRPCBase that uses the RPC system.
 * (The other implementation, LeaderRPCMock, is only used for testing.)
 */
class LeaderRPC : public LeaderRPCBase {
  public:
    /**
     * Constructor.
     * \param hosts
     *      Describe the servers to connect to. This class assumes that
     *      refreshing 'hosts' will result in a random host that might be the
     *      current cluster leader.
     * \param clusterUUID
     *      Keeps track of the unique ID for this cluster, if known.
     * \param sessionCreationBackoff
     *      Used to rate-limit new TCP connections.
     * \param sessionManager
     *      Used to create new sessions.
     */
    LeaderRPC(const RPC::Address& hosts,
              SessionManager::ClusterUUID& clusterUUID,
              Backoff& sessionCreationBackoff,
              SessionManager& sessionManager);

    /// Destructor.
    ~LeaderRPC();

    /// See LeaderRPCBase::call.
    Status call(OpCode opCode,
                const google::protobuf::Message& request,
                google::protobuf::Message& response,
                TimePoint timeout);

    /// See LeaderRPCBase::makeCall().
    std::unique_ptr<LeaderRPCBase::Call> makeCall();

  private:

    /// See LeaderRPCBase::Call.
    class Call : public LeaderRPCBase::Call {
      public:
        explicit Call(LeaderRPC& leaderRPC);
        ~Call();
        void start(OpCode opCode,
                   const google::protobuf::Message& request,
                   TimePoint timeout);
        void cancel();
        Status wait(google::protobuf::Message& response,
                    TimePoint timeout);
        LeaderRPC& leaderRPC;
        /**
         * Copy of leaderSession when the RPC was started (might have changed
         * since).
         */
        std::shared_ptr<RPC::ClientSession> cachedSession;
        /**
         * RPC object which may be canceled.
         */
        RPC::ClientRPC rpc;
    };

    /**
     * Return a session connected to the most likely cluster leader, creating
     * it if necessary.
     * \param timeout
     *      After this time has elapsed, stop trying to initiate the connection
     *      and return an invalid session.
     * \return
     *      Session on which to execute RPCs.
     */
    std::shared_ptr<RPC::ClientSession>
    getSession(TimePoint timeout);

    /**
     * Notify this class that an RPC on the given session failed. This will
     * usually cause this class to connect to a random server next time
     * getSession() is called.
     * \param cachedSession
     *      Session previously returned by getSession(). This is used to detect
     *      races in which some other thread has already solved the problem.
     */
    void
    reportFailure(std::shared_ptr<RPC::ClientSession> cachedSession);

    /**
     * Notify this class that a non-leader server rejected an RPC. This will
     * usually cause this class to connect to a random server next time
     * getSession() is called.
     * \param cachedSession
     *      Session previously returned by getSession(). This is used to detect
     *      races in which some other thread has already solved the problem.
     */
    void
    reportNotLeader(std::shared_ptr<RPC::ClientSession> cachedSession);

    /**
     * Notify this class that an RPC on the given session was redirected by a
     * non-leader server. This will usually cause this class to connect to the
     * given host the next time getSession() is called.
     * \param cachedSession
     *      Session previously returned by getSession(). This is used to detect
     *      races in which some other thread has already solved the problem.
     * \param host
     *      Address of the server that is likely the leader.
     */
    void
    reportRedirect(std::shared_ptr<RPC::ClientSession> cachedSession,
                   const std::string& host);

    /**
     * Notify this class that an RPC on the given session reached a leader.
     * This is just here for debug log messages.
     * \param cachedSession
     *      Session previously returned by getSession(). This is used to detect
     *      races in which some other thread has already solved any problems.
     */
    void
    reportSuccess(std::shared_ptr<RPC::ClientSession> cachedSession);

    /**
     * Keeps track of the unique ID for this cluster, if known.
     */
    SessionManager::ClusterUUID& clusterUUID;

    /**
     * Used to rate-limit the creation of ClientSession objects (TCP
     * connections).
     */
    Backoff& sessionCreationBackoff;

    /**
     * Used to create new sessions.
     */
    SessionManager& sessionManager;

    /**
     * Protects all of the following member variables in this class.
     */
    std::mutex mutex;

    /**
     * Set to true when some thread is already initiating a new session.
     * When this is already true, other threads wait on #connected
     * rather than starting additional sessions.
     */
    bool isConnecting;

    /**
     * Notified when #isConnecting becomes false (when #leaderSession is set).
     */
    Core::ConditionVariable connected;

    /**
     * An address referring to the hosts in the LogCabin cluster. A random host
     * is selected from here when this class doesn't know who the cluster
     * leader is.
     */
    RPC::Address hosts;

    /**
     * If nonempty, the address of the server that is likely to be the
     * current leader.
     */
     std::string leaderHint;

    /**
     * The goal is to get this session connected to the cluster leader.
     * This is never null, but it might sometimes point to the wrong host.
     */
    std::shared_ptr<RPC::ClientSession> leaderSession;

    /**
     * The number of attempted RPCs that have not successfully reached a leader
     * since the last time an RPC did. Used for rate-limiting and summarizing
     * log messages: they're only printed when this number reaches a power of
     * two.
     */
    uint64_t failuresSinceLastSuccess;
};

} // namespace LogCabin::Client
} // namespace LogCabin

#endif /* LOGCABIN_CLIENT_LEADERRPC_H */
