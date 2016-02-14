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

/**
 * \file
 * This file contains the headers used in all high-level RPCs.
 */

#include <cinttypes>
#include <ostream>

#ifndef LOGCABIN_RPC_PROTOCOL_H
#define LOGCABIN_RPC_PROTOCOL_H

namespace LogCabin {
namespace RPC {
namespace Protocol {

/**
 * This is the first part of the request header that RPC clients send, common
 * to all versions of the protocol. RPC servers can always expect to receive
 * this and RPC clients must always send this.
 * This needs to be separate struct because when a server receives a request,
 * it does not know the type of the request header, as that depends on its
 * version.
 */
struct RequestHeaderPrefix {
    /**
     * Convert the contents to host order from big endian (how this header
     * should be transferred on the network).
     */
    void fromBigEndian();
    /**
     * Convert the contents to big endian (how this header should be
     * transferred on the network) from host order.
     */
    void toBigEndian();

    /**
     * This is the version of the protocol. It should always be set to 1 for
     * now.
     */
    uint8_t version;

} __attribute__((packed));

/**
 * In version 1 of the protocol, this is the header format for requests from
 * clients to servers.
 */
struct RequestHeaderVersion1 {
    /**
     * Convert the contents to host order from big endian (how this header
     * should be transferred on the network).
     * \warning
     *      This does not modify #prefix.
     */
    void fromBigEndian();
    /**
     * Convert the contents to big endian (how this header should be
     * transferred on the network) from host order.
     * \warning
     *      This does not modify #prefix.
     */
    void toBigEndian();

    /**
     * This is common to all versions of the protocol. RPC servers can always
     * expect to receive this and RPC clients must always send this.
     */
    RequestHeaderPrefix prefix;

    /**
     * This identifies which Service the RPC is destined for.
     * See Protocol::Common::ServiceId.
     */
    uint16_t service;

    /**
     * This field tells the service what service-specific errors the client
     * understands. Clients should remain backwards-compatible, so that newer
     * clients can understand older errors. Services should take care not to
     * send a client a service-specific error that it doesn't understand.
     */
    uint8_t serviceSpecificErrorVersion;

    /**
     * This identifies which RPC is being executed, scoped to the service.
     */
    uint16_t opCode;

} __attribute__((packed));

/**
 * The status codes returned in server responses.
 */
enum class Status : uint8_t {

    /**
     * The service processed the request and returned a valid protocol buffer
     * with the results.
     */
    OK = 0,

    /**
     * An error specific to the particular service. The format of the remainder
     * of the message is specific to the particular service.
     */
    SERVICE_SPECIFIC_ERROR = 1,

    /**
     * The server did not like the version number provided in the request
     * header. If the client gets this, it should fall back to an older version
     * number or crash.
     */
    INVALID_VERSION = 2,

    /**
     * The server does not have the requested service.
     */
    INVALID_SERVICE = 3,

    /**
     * The server did not like the RPC request. Either it specified an opCode
     * the server didn't understand or a request protocol buffer the server
     * couldn't accept. The client should avoid ever getting this by
     * negotiating with the server about which version of the RPC protocol to
     * use.
     */
    INVALID_REQUEST = 4,

};

/// Output a Status to a stream. Improves gtest error messages.
::std::ostream&
operator<<(::std::ostream& stream, Status status);

/**
 * This is the first part of the response header that servers send, common to
 * all versions of the protocol. RPC clients can always expect to receive this
 * and RPC servers must always send this.
 * This needs to be separate struct because when a client receives a response,
 * it might have a status of INVALID_VERSION, in which case the client may not
 * assume anything about the remaining bytes in the message.
 */
struct ResponseHeaderPrefix {
    /**
     * Convert the contents to host order from big endian (how this header
     * should be transferred on the network).
     */
    void fromBigEndian();
    /**
     * Convert the contents to big endian (how this header should be
     * transferred on the network) from host order.
     */
    void toBigEndian();

    /**
     * The error code returned by the server.
     */
    Status status;

    // If status != INVALID_VERSION, the response should be cast
    // to the appropriate ResponseHeaderVersion# struct.
} __attribute__((packed));

/**
 * In version 1 of the protocol, this is the header format for RPC responses.
 */
struct ResponseHeaderVersion1 {
    /**
     * Convert the contents to host order from big endian (how this header
     * should be transferred on the network). This is just here for
     * completeness, as this header has no fields of its own.
     * \warning
     *      This does not modify #prefix.
     */
    void fromBigEndian();
    /**
     * Convert the contents to big endian (how this header should be
     * transferred on the network) from host order. This is just here for
     * completeness, as this header has no fields of its own.
     * \warning
     *      This does not modify #prefix.
     */
    void toBigEndian();

    /**
     * This is common to all versions of the protocol. RPC clients can always
     * expect to receive this and RPC servers must always send this.
     */
    ResponseHeaderPrefix prefix;

} __attribute__((packed));

} // namespace LogCabin::RPC::Protocol
} // namespace LogCabin::RPC
} // namespace LogCabin

#endif // LOGCABIN_RPC_PROTOCOL_H
