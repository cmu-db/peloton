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

#include <cinttypes>
#include <google/protobuf/message.h>

#include "RPC/Protocol.h"
#include "RPC/OpaqueServerRPC.h"

#ifndef LOGCABIN_RPC_SERVERRPC_H
#define LOGCABIN_RPC_SERVERRPC_H

namespace LogCabin {
namespace RPC {

/**
 * This class represents the server side of a remote procedure call.
 * A Server creates an instance when an RPC is initiated. This is used to send
 * the reply.
 *
 * This class may be used from any thread, but each object is meant to be
 * accessed by only one thread at a time.
 */
class ServerRPC {

    /**
     * Constructor for ServerRPC. This is called by Server only.
     */
    explicit ServerRPC(OpaqueServerRPC opaqueRPC);

  public:
    /**
     * Default constructor for empty RPC that can't be replied to.
     * This is useful as a placeholder for a real ServerRPC.
     */
    ServerRPC();

    /**
     * Move constructor.
     */
    ServerRPC(ServerRPC&& other);

    /**
     * Destructor.
     */
    ~ServerRPC();

    /**
     * Move assignment.
     */
    ServerRPC& operator=(ServerRPC&& other);

    /**
     * Returns whether this RPC is waiting for a reply.
     * \return
     *      True if the owner needs to take action based on this RPC;
     *      false if the RPC is not valid or has already been replied to. If
     *      this returns false, the caller should discard this ServerRPC
     *      object.
     */
    bool needsReply() const {
        return active;
    }

    /**
     * This identifies which Service the RPC is destined for.
     * The Server class uses this to dispatch to the appropriate Service.
     */
    uint16_t getService() const {
        return service;
    }

    /**
     * This tells the Service what service-specific errors the client
     * understands. Services should take care not to send a client a
     * service-specific error that it doesn't understand.
     */
    uint8_t getServiceSpecificErrorVersion() const {
        return serviceSpecificErrorVersion;
    }

    /**
     * Return which RPC is being executed, scoped to the service.
     */
    uint16_t getOpCode() const {
        return opCode;
    }

    /**
     * Parse the request out of the RPC.
     * \param[out] request
     *      An empty protocol buffer the request will be be unpacked into.
     * \return
     *      True if 'request' contains a valid RPC request which needs to be
     *      handled; false otherwise. If this returns false, the caller should
     *      discard this ServerRPC object.
     */
    bool getRequest(google::protobuf::Message& request);

    /**
     * Copy the request out of the RPC.
     * \param[out] buffer
     *      An empty buffer that the request will be copied into.
     * \return
     *      True if 'request' contains a valid RPC request which needs to be
     *      handled; false otherwise. If this returns false, the caller should
     *      discard this ServerRPC object.
     */
    bool getRequest(Core::Buffer& buffer) const;

    /**
     * Send a normal response back to the client.
     * \param payload
     *      A protocol buffer to serialize into the response.
     */
    void reply(const google::protobuf::Message& payload);

    /**
     * Send a service-specific error back to the client.
     * \param serviceSpecificError
     *      Details explaining the error to send back to the client.
     */
    void returnError(const google::protobuf::Message& serviceSpecificError);

    /**
     * Reject the RPC on the grounds that it specifies an invalid service ID.
     */
    void rejectInvalidService();

    /**
     * Reject the RPC on the grounds that it specifies an invalid request.
     */
    void rejectInvalidRequest();

    /**
     * Close the session on which this request originated. This is an impolite
     * thing to do to a client but can be useful occasionally, for example for
     * testing.
     */
    void closeSession();

  private:
    /**
     * Reject the RPC.
     * \param status
     *      This should be INVALID_VERSION, INVALID_SERVICE, or
     *      INVALID_REQUEST.
     */
    void reject(RPC::Protocol::Status status);

    /**
     * The underlying transport-level RPC object. It doesn't know how to
     * interpret the raw bytes of the RPC, but it gets them from here to there.
     */
    OpaqueServerRPC opaqueRPC;

    /**
     * Set to true if the RPC needs a reply, false otherwise.
     */
    bool active;

    /// See getService().
    uint16_t service;
    /// See getServiceSpecificErrorVersion().
    uint8_t serviceSpecificErrorVersion;
    /// See getOpCode().
    uint16_t opCode;

    friend class Server;

    // ServerRPC is non-copyable.
    ServerRPC(const ServerRPC&) = delete;
    ServerRPC& operator=(const ServerRPC&) = delete;

}; // class ServerRPC

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_SERVERRPC_H */
