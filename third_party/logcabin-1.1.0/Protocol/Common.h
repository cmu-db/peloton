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
 * This file contains declarations useful to all LogCabin RPCs.
 */

#ifndef LOGCABIN_PROTOCOL_COMMON_H
#define LOGCABIN_PROTOCOL_COMMON_H

#include <cstdint>

namespace LogCabin {
namespace Protocol {
namespace Common {

/**
 * The default TCP port on which LogCabin servers serve RPCs.
 * TCP port 5254 is reserved by IANA for LogCabin as of April 2015.
 */
enum { DEFAULT_PORT = 5254 };

/**
 * Reserved MessageSocket::MessageID values.
 * No application-level RPCs will ever be assigned these IDs.
 */
enum {
    /**
     * Messages that are used to check the server's liveness.
     */
    PING_MESSAGE_ID = ~0UL,
    /**
     * Messages used to check which versions of the MessageSocket framing
     * protocol the server supports.
     */
    VERSION_MESSAGE_ID = ~0UL - 1,
};

/**
 * Defines request and response types for messages sent using ID VERSION_MESSAGE_ID,
 * used to check which versions of the MessageSocket framing protocol the
 * server supports.
 */
namespace VersionMessage {
struct Request {
    // empty
} __attribute__((packed));

struct Response {
    /**
     * The largest version of the MessageSocket framing protocol that the
     * server understands. Requests with larger versions will cause the server
     * to close the connection. Big endian.
     */
    uint16_t maxVersionSupported;
} __attribute__((packed));

} // namespace VersionMessage

/**
 * The maximum number of bytes per RPC request or response, including these
 * headers. This is set to slightly over 1 MB because the maximum size of log
 * entries is 1 MB.
 */
enum { MAX_MESSAGE_LENGTH = 1024 + 1024 * 1024 };

// This is not an enum class because it is usually used as a uint16_t;
// enum class is too strict about conversions.
namespace ServiceId {
enum {

    /**
     * The service that LogCabin client applications communicate with via the
     * client library.
     */
    CLIENT_SERVICE = 1,

    /**
     * The service that LogCabin servers use to communicate with each other.
     * The consensus protocol runs over this service.
     */
    RAFT_SERVICE = 2,

    /**
     * Used by the logcabinctl client to query and change the internal state of
     * a server.
     */
    CONTROL_SERVICE = 3,
};
}

} // namespace LogCabin::Protocol::Common
} // namespace LogCabin::Protocol
} // namespace LogCabin

#endif // LOGCABIN_PROTOCOL_COMMON_H
