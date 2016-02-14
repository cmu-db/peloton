/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "Core/Buffer.h"
#include "Core/Time.h"

#ifndef LOGCABIN_RPC_OPAQUECLIENTRPC_H
#define LOGCABIN_RPC_OPAQUECLIENTRPC_H

namespace LogCabin {
namespace RPC {

class ClientSession; // forward declaration

/**
 * This class represents an asynchronous remote procedure call. A ClientSession
 * returns an instance when an RPC is initiated; this can be used to wait for
 * and retrieve the reply.
 */
class OpaqueClientRPC {
  public:
    /// Clock used for timeouts.
    typedef Core::Time::SteadyClock Clock;
    /// Type for absolute time values used for timeouts.
    typedef Clock::time_point TimePoint;

    /**
     * State of the RPC.
     */
    enum class Status {
        /**
         * The RPC is still in progress.
         */
        NOT_READY,
        /**
         * The RPC has completed successfully.
         */
        OK,
        /**
         * The RPC has failed with an error (see #getErrorMessage()).
         */
        ERROR,
        /**
         * The RPC was aborted using #cancel().
         */
        CANCELED,
    };

    /**
     * Default constructor. This doesn't create a valid RPC, but it is useful
     * as a placeholder.
     */
    OpaqueClientRPC();

    /**
     * Move constructor.
     */
    OpaqueClientRPC(OpaqueClientRPC&&);

    /**
     * Destructor.
     */
    ~OpaqueClientRPC();

    /**
     * Move assignment.
     */
    OpaqueClientRPC& operator=(OpaqueClientRPC&&);

    /**
     * Abort the RPC.
     * The caller is no longer interested in its reply.
     */
    void cancel();

    /**
     * If an error has occurred, return a message describing that error.
     *
     * All errors indicate that it is unknown whether or not the server
     * executed the RPC. Unless the RPC was canceled with #cancel(), the
     * ClientSession has been disconnected and is no longer useful for
     * initiating new RPCs.
     *
     * \return
     *      If an error has occurred, a message describing that error.
     *      Otherwise, an empty string.
     */
    std::string getErrorMessage() const;

    /**
     * See #Status.
     */
    Status getStatus() const;

    /**
     * Look at the reply buffer.
     *
     * \return
     *      If the reply is already available and there were no errors, returns
     *      a pointer to the reply buffer inside this OpaqueClientRPC object.
     *      Otherwise, returns NULL.
     */
    Core::Buffer* peekReply();

    /**
     * Block until the reply is ready, an error has occurred, or the given
     * timeout elapses.
     *
     * This may be used from worker threads only, because OpaqueClientRPC
     * objects rely on the event loop servicing their ClientSession in order to
     * make progress.
     *
     * \param timeout
     *      After this time has elapsed, stop waiting and return. The RPC's
     *      results will probably not be available yet in this case (status
     *      will be NOT_READY).
     */
    void waitForReply(TimePoint timeout);

  private:

    /**
     * Update the fields of this object if the RPC has not completed.
     * Must be called with the lock held.
     */
    void update();

    /**
     * Protects all the members of this class.
     */
    mutable std::mutex mutex;

    /**
     * The session on which this RPC is executing.
     * The session itself will reset this field once the reply has been
     * received to eagerly drop its own reference count.
     */
    std::shared_ptr<ClientSession> session;

    /**
     * A token given to the session to look up new information about the
     * progress of this RPC's reply.
     */
    uint64_t responseToken;

    /**
     * See #Status.
     */
    Status status;

    /**
     * The payload of a successful reply, once available.
     * This becomes valid when #status is OK.
     */
    Core::Buffer reply;

    /**
     * If an error occurred in the RPC then this holds the error message;
     * otherwise, this is the empty string.
     */
    std::string errorMessage;

    // The ClientSession class fills in the members of this object.
    friend class ClientSession;

    // OpaqueClientRPC is non-copyable.
    OpaqueClientRPC(const OpaqueClientRPC&) = delete;
    OpaqueClientRPC& operator=(const OpaqueClientRPC&) = delete;

}; // class OpaqueClientRPC

/**
 * Output an OpaqueClientRPC::Status to a stream.
 * This is helpful for google test output.
 */
::std::ostream&
operator<<(::std::ostream& os, OpaqueClientRPC::Status status);


} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_OPAQUECLIENTRPC_H */
