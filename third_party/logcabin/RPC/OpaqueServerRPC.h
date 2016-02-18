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

#include <memory>

#include "Core/Buffer.h"
#include "RPC/MessageSocket.h"
#include "RPC/OpaqueServer.h"

#ifndef LOGCABIN_RPC_OPAQUESERVERRPC_H
#define LOGCABIN_RPC_OPAQUESERVERRPC_H

namespace LogCabin {
namespace RPC {

/**
 * This class represents the server side of a remote procedure call.
 * An OpaqueServer returns an instance when an RPC is initiated. This is used
 * to send the reply.
 *
 * This class may be used from any thread, but each object is meant to be
 * accessed by only one thread at a time.
 */
class OpaqueServerRPC {
    /**
     * Constructor for OpaqueServerRPC. This is called by OpaqueServer.
     * \param socket
     *      The socket on which to send the reply.
     * \param messageId
     *      The message ID received with the request.
     * \param request
     *      The RPC request received from the client.
     */
    OpaqueServerRPC(
            std::weak_ptr<OpaqueServer::SocketWithHandler> socket,
            MessageSocket::MessageId messageId,
            Core::Buffer request);

  public:
    /**
     * Default constructor for empty RPC that can't be replied to.
     * This is useful as a placeholder for a real OpaqueServerRPC.
     */
    OpaqueServerRPC();


    /**
     * Move constructor.
     */
    OpaqueServerRPC(OpaqueServerRPC&& other);

    /**
     * Destructor.
     */
    ~OpaqueServerRPC();

    /**
     * Move assignment.
     */
    OpaqueServerRPC& operator=(OpaqueServerRPC&& other);

    /**
     * Close the session on which this request originated.
     * This is an impolite thing to do to a client but can be useful
     * occasionally, for example for testing.
     */
    void closeSession();

    /**
     * Send the response back to the client.
     * This will reset #response to an empty state, and further replies on this
     * object will not do anything.
     */
    void sendReply();

    /**
     * The RPC request received from the client.
     */
    Core::Buffer request;

    /**
     * The reply to the RPC, to send back to the client.
     */
    Core::Buffer response;

  private:
    /**
     * The socket on which to send the reply.
     */
    std::weak_ptr<OpaqueServer::SocketWithHandler> socket;

    /**
     * The message ID received with the request. This should be sent back to
     * the client with the response so that the client can match up the
     * response with the right ClientRPC object.
     */
    MessageSocket::MessageId messageId;

    /**
     * This is used in unit testing only. During normal operation, this is
     * always NULL. If this is not NULL when sendReply() is invoked, the reply
     * will be moved here.
     */
    Core::Buffer* responseTarget;

    // The OpaqueServer class uses the private members of this object.
    friend class OpaqueServer;

    // OpaqueServerRPC is non-copyable.
    OpaqueServerRPC(const OpaqueServerRPC&) = delete;
    OpaqueServerRPC& operator=(const OpaqueServerRPC&) = delete;

}; // class OpaqueServerRPC

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_OPAQUESERVERRPC_H */
