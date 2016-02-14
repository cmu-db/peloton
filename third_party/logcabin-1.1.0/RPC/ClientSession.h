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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "Core/Buffer.h"
#include "Core/ConditionVariable.h"
#include "Core/Config.h"
#include "Event/Timer.h"
#include "RPC/Address.h"
#include "RPC/OpaqueClientRPC.h"
#include "RPC/MessageSocket.h"

#ifndef LOGCABIN_RPC_CLIENTSESSION_H
#define LOGCABIN_RPC_CLIENTSESSION_H

namespace LogCabin {

// forward declaration
namespace Event {
class Loop;
};

namespace RPC {

/**
 * A ClientSession is used to initiate OpaqueClientRPCs. It encapsulates a
 * connection to a server. Sessions can be relatively expensive to create, so
 * clients should keep them around.
 */
class ClientSession {
  public:
    /// Clock used for timeouts.
    typedef Address::Clock Clock;
    /// Type for absolute time values used for timeouts.
    typedef Address::TimePoint TimePoint;

  private:
    /**
     * This constructor is private because the class must be allocated in a
     * particular way. See #makeSession().
     */
    ClientSession(Event::Loop& eventLoop,
                  const Address& address,
                  uint32_t maxMessageLength,
                  TimePoint timeout,
                  const Core::Config& config);
  public:
    /**
     * Return a new ClientSession object. This object is managed by a
     * std::shared_ptr to ensure that it remains alive while there are
     * outstanding RPCs.
     *
     * This should only be used from worker threads, as it invokes possibly
     * long-running syscalls.
     *
     * \param eventLoop
     *      Event::Loop that will be used to find out when the underlying
     *      socket may be read from or written to without blocking.
     * \param address
     *      The RPC server address on which to connect.
     * \param maxMessageLength
     *      The maximum number of bytes to allow per request/response. This
     *      exists to limit the amount of buffer space a single RPC can use.
     *      Attempting to send longer requests will PANIC; attempting to
     *      receive longer requests will disconnect the underlying socket.
     * \param timeout
     *      After this time has elapsed, stop trying to initiate the connection
     *      and leave the session in an error state.
     * \param config
     *      General settings. This object does not keep a reference.
     */
    static std::shared_ptr<ClientSession>
    makeSession(Event::Loop& eventLoop,
                const Address& address,
                uint32_t maxMessageLength,
                TimePoint timeout,
                const Core::Config& config);

    /**
     * Return a ClientSession object that's already in an error state. This can
     * be useful for delaying errors until an RPC is waited on.
     * \param eventLoop
     *      Ignored but usually readily available to callers and needed to
     *      satisfy type requirements.
     * \param errorMessage
     *      Description of the error, as will be returned by getErrorMessage()
     *      later.
     */
    static std::shared_ptr<ClientSession>
    makeErrorSession(Event::Loop& eventLoop,
                     const std::string& errorMessage);

    /**
     * Destructor.
     */
    ~ClientSession();

    /**
     * Initiate an RPC.
     * This method is safe to call from any thread.
     * \param request
     *      The contents of the RPC request.
     * \return
     *      This is be used to wait for and retrieve the reply to the RPC.
     */
    OpaqueClientRPC sendRequest(Core::Buffer request);

    /**
     * If the socket has been disconnected, return a descriptive message.
     * The suggested way to detect errors is to wait until an RPC returns an
     * error. This method can be used to detect errors earlier.
     *
     * This method is safe to call from any thread.
     *
     * \return
     *      If an error has occurred, a message describing that error.
     *      Otherwise, an empty string.
     */
    std::string getErrorMessage() const;

    /**
     * Return a string describing this session. It will include the address of
     * the server and, if the session has an error, the error message.
     */
    std::string toString() const;

  private:

    /**
     * This handles events from #messageSocket.
     */
    class MessageSocketHandler : public MessageSocket::Handler {
      public:
        explicit MessageSocketHandler(ClientSession& clientSession);
        void handleReceivedMessage(MessageId messageId, Core::Buffer message);
        void handleDisconnect();
        ClientSession& session;
    };

    /**
     * This contains an expected response for a OpaqueClientRPC object.
     * This is created when the OpaqueClientRPC is created; it is deleted when
     * the OpaqueClientRPC object is either canceled or updated with a
     * response/error.
     */
    struct Response {
        /**
         * Constructor.
         */
        Response();
        /**
         * Current state of the RPC.
         */
        enum {
          /**
           * Waiting for a reply from the server.
           */
          WAITING,
          /**
           * Received a reply (find it in #reply).
           */
          HAS_REPLY,
          /**
           * The RPC has been canceled by another thread.
           */
          CANCELED,
        } status;
        /**
         * The contents of the response. This is valid when
         * #status is HAS_REPLY.
         */
        Core::Buffer reply;
        /**
         * If true, a thread is blocked waiting on #ready,
         * and this object may not be deleted.
         */
        bool hasWaiter;
        /**
         * OpaqueClientRPC objects wait on this condition variable inside of
         * wait(). It is notified when a new response arrives, the session
         * is disconnected, or the RPC is canceled.
         */
        Core::ConditionVariable ready;
    };

    /**
     * This is used to time out RPCs and sessions when the server is no longer
     * responding. After a timeout period, the client will send a ping to the
     * server. If no response is received within another timeout period, the
     * session is closed.
     */
    class Timer : public Event::Timer {
      public:
        explicit Timer(ClientSession& session);
        void handleTimerEvent();
        ClientSession& session;
    };

    // The cancel(), update(), and wait() methods are used by OpaqueClientRPC.
    friend class OpaqueClientRPC;

    /**
     * Called by the RPC when it is no longer interested in its response.
     *
     * This may be called while holding the RPC's lock.
     *
     * TODO(ongaro): It'd be nice to cancel sending the request if it hasn't
     * already gone out, but I guess that's going to be a pretty rare case.
     */
    void cancel(OpaqueClientRPC& rpc);

    /**
     * Called by the RPC when it wants to be learn of its response
     * (non-blocking).
     *
     * This must be called while holding the RPC's lock.
     */
    void update(OpaqueClientRPC& rpc);

    /**
     * Called by the RPC to wait for its response (blocking). The caller should
     * call update() after this returns to learn of the response.
     *
     * This must not be called while holding the RPC's lock.
     * \param rpc
     *      Wait for response to this.
     * \param timeout
     *      After this time has elapsed, stop waiting and return. The RPC's
     *      results will probably not be available yet in this case.
     */
    void wait(const OpaqueClientRPC& rpc, TimePoint timeout);

    /**
     * This is used to keep this object alive while there are outstanding RPCs.
     */
    std::weak_ptr<ClientSession> self;

    /**
     * The number of nanoseconds to wait until the client gets suspicious
     * about the server not responding. After this amount of time elapses, the
     * client will send a ping to the server. If no response is received within
     * another PING_TIMEOUT_NS milliseconds, the session is closed.
     *
     * TODO(ongaro): How should this value be chosen?
     * Ideally, you probably want this to be set to something like the 99-th
     * percentile of your RPC latency.
     *
     * TODO(ongaro): How does this interact with TCP?
     */
    const uint64_t PING_TIMEOUT_NS;

    /**
     * The event loop that is used for non-blocking I/O.
     */
    Event::Loop& eventLoop;

    /**
     * The RPC server address provided to the constructor.
     */
    const Address address;

    /**
     * Receives events from #messageSocket.
     */
    MessageSocketHandler messageSocketHandler;

    /**
     * This is used to time out RPCs and sessions when the server is no longer
     * responding. See Timer.
     */
    Timer timer;

    /**
     * This mutex protects several members of this class:
     *  - #nextMessageId
     *  - #responses
     *  - #errorMessage
     *  - #numActiveRPCs
     *  - #activePing
     */
    mutable std::mutex mutex;

    /**
     * The message ID to assign to the next RPC. These start at 0 and
     * increment from there.
     */
    MessageSocket::MessageId nextMessageId;

    /**
     * A map from MessageId to Response objects that is used to store the
     * response to RPCs and look it up for OpaqueClientRPC objects. The
     * Response objects mapped to must be deleted manually when removed from
     * this map (gcc 4.4 doesn't support mapping to non-copyable objects).
     */
    std::unordered_map<MessageSocket::MessageId, Response*> responses;

    /**
     * If this session is disconnected then this holds the error message.
     * All new RPCs will be immediately 'ready' with this error message.
     * Otherwise, this is the empty string.
     */
    std::string errorMessage;

    /**
     * The number of outstanding RPC requests that have been sent but whose
     * responses have not yet been received. This does not include ping
     * requests sent by the #timer (which aren't real RPCs).
     * This is used to determine when to schedule the timer: the timer is
     * scheduled if numActiveRPCs is non-zero.
     */
    uint32_t numActiveRPCs;

    /**
     * When numActiveRPCs is > 0, this field indicates that we are waiting for
     * a ping response as evidence that the server is still alive.
     * When numActiveRPCs = 0, this field is undefined.
     */
    bool activePing;

    /**
     * The MessageSocket used to send RPC requests and receive RPC responses.
     * This may be NULL if the socket was never created. In this case,
     * #errorMessage will be set.
     */
    std::unique_ptr<MessageSocket> messageSocket;

    /**
     * Registers timer with the event loop.
     */
    Event::Timer::Monitor timerMonitor;

    /**
     * Usually set to connect() but mocked out in some unit tests.
     */
    static std::function<
        int(int sockfd,
            const struct sockaddr *addr,
            socklen_t addrlen)> connectFn;

    // ClientSession is non-copyable.
    ClientSession(const ClientSession&) = delete;
    ClientSession& operator=(const ClientSession&) = delete;
}; // class ClientSession

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_CLIENTSESSION_H */
