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

#include "RPC/OpaqueServerRPC.h"

namespace LogCabin {
namespace RPC {

OpaqueServerRPC::OpaqueServerRPC()
    : request()
    , response()
    , socket()
    , messageId(~0UL)
    , responseTarget(NULL)
{
}

OpaqueServerRPC::OpaqueServerRPC(
        std::weak_ptr<OpaqueServer::SocketWithHandler> socket,
        MessageSocket::MessageId messageId,
        Core::Buffer request)
    : request(std::move(request))
    , response()
    , socket(socket)
    , messageId(messageId)
    , responseTarget(NULL)
{
}

OpaqueServerRPC::OpaqueServerRPC(OpaqueServerRPC&& other)
    : request(std::move(other.request))
    , response(std::move(other.response))
    , socket(std::move(other.socket))
    , messageId(std::move(other.messageId))
    , responseTarget(std::move(other.responseTarget))
{
}

OpaqueServerRPC::~OpaqueServerRPC()
{
}

OpaqueServerRPC&
OpaqueServerRPC::operator=(OpaqueServerRPC&& other)
{
    request = std::move(other.request);
    response = std::move(other.response);
    socket = std::move(other.socket);
    messageId = std::move(other.messageId);
    responseTarget = std::move(other.responseTarget);
    return *this;
}

void
OpaqueServerRPC::closeSession()
{
    std::shared_ptr<OpaqueServer::SocketWithHandler> socketRef = socket.lock();
    if (socketRef)
        socketRef->monitor.close();
    socket.reset();
    responseTarget = NULL;
}

void
OpaqueServerRPC::sendReply()
{
    std::shared_ptr<OpaqueServer::SocketWithHandler> socketRef = socket.lock();
    if (socketRef) {
        socketRef->monitor.sendMessage(messageId, std::move(response));
    } else {
        // During normal operation, this indicates that either the socket has
        // been disconnected or the reply has already been sent.

        // For unit testing only, we can store replies from mock RPCs
        // that have no sessions.
        if (responseTarget != NULL) {
            *responseTarget = std::move(response);
            responseTarget = NULL;
        }

        // Drop the reply on the floor.
        response.reset();
    }
    // Prevent the server from replying again.
    socket.reset();
}

} // namespace LogCabin::RPC
} // namespace LogCabin
