/* Copyright (c) 2011-2014 Stanford University
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

#include <errno.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Protocol/Common.h"
#include "RPC/Address.h"
#include "RPC/OpaqueServer.h"
#include "RPC/OpaqueServerRPC.h"

namespace LogCabin {
namespace RPC {


////////// OpaqueServer::MessageSocketHandler //////////

OpaqueServer::MessageSocketHandler::MessageSocketHandler(OpaqueServer* server)
    : server(server)
    , self()
{
}

void
OpaqueServer::MessageSocketHandler::handleReceivedMessage(
        MessageId messageId,
        Core::Buffer message)
{
    if (server == NULL)
        return;
    switch (messageId) {
        case Protocol::Common::PING_MESSAGE_ID: {
            std::shared_ptr<SocketWithHandler> socketRef = self.lock();
            if (socketRef) { // expect so, since we're receiving messages
                VERBOSE("Responding to ping");
                socketRef->monitor.sendMessage(messageId,
                                               Core::Buffer());
            }
            break;
        }
        case Protocol::Common::VERSION_MESSAGE_ID: {
            std::shared_ptr<SocketWithHandler> socketRef = self.lock();
            if (socketRef) { // expect so, since we're receiving messages
                VERBOSE("Responding to version request "
                        "(this server supports max version %u)",
                        MessageSocket::MAX_VERSION_SUPPORTED);
                using Protocol::Common::VersionMessage::Response;
                Response* response = new Response();
                response->maxVersionSupported =
                    htobe16(MessageSocket::MAX_VERSION_SUPPORTED);
                socketRef->monitor.sendMessage(
                    messageId,
                    Core::Buffer(response, sizeof(*response),
                                 Core::Buffer::deleteObjectFn<Response*>));
            }
            break;
        }
        default: { // normal RPC request
            VERBOSE("Handling RPC");
            OpaqueServerRPC rpc(self, messageId, std::move(message));
            server->rpcHandler.handleRPC(std::move(rpc));
        }
    }
}

void
OpaqueServer::MessageSocketHandler::handleDisconnect()
{
    VERBOSE("Disconnected from client");
    std::shared_ptr<SocketWithHandler> socketRef = self.lock();
    if (server != NULL && socketRef) {
        // This drops the reference count on the socket. It may cause the
        // SocketWithHandler object (which includes this object) to be
        // destroyed when 'socketRef' goes out of scope.
        server->sockets.erase(socketRef);
        server = NULL;
    }
}


////////// OpaqueServer::SocketWithHandler //////////

std::shared_ptr<OpaqueServer::SocketWithHandler>
OpaqueServer::SocketWithHandler::make(OpaqueServer* server, int fd)
{
    std::shared_ptr<SocketWithHandler> socket(
        new SocketWithHandler(server, fd));
    socket->handler.self = socket;
    return socket;
}

OpaqueServer::SocketWithHandler::SocketWithHandler(
        OpaqueServer* server,
        int fd)
    : handler(server)
    , monitor(handler, server->eventLoop, fd, server->maxMessageLength)
{
}

OpaqueServer::SocketWithHandler::~SocketWithHandler() {
    // 'handler' shouldn't have access to 'monitor' while/after 'monitor' is
    // destroyed. Clear the reference here, then C++ will destroy 'monitor',
    // then 'handler'.
    handler.self.reset();
}


////////// OpaqueServer::BoundListener //////////

OpaqueServer::BoundListener::BoundListener(
        OpaqueServer& server,
        int fd)
    : Event::File(fd)
    , server(server)
{
}

void
OpaqueServer::BoundListener::handleFileEvent(uint32_t events)
{
    int clientfd = accept4(fd, NULL, NULL, SOCK_NONBLOCK|SOCK_CLOEXEC);
    if (clientfd < 0) {
        PANIC("Could not accept connection on fd %d: %s",
              fd, strerror(errno));
    }

    server.sockets.insert(SocketWithHandler::make(&server, clientfd));
}


////////// OpaqueServer::BoundListenerWithMonitor //////////

OpaqueServer::BoundListenerWithMonitor::BoundListenerWithMonitor(
        OpaqueServer& server,
        int fd)
    : handler(server, fd)
    , monitor(server.eventLoop, handler, EPOLLIN)
{
}

OpaqueServer::BoundListenerWithMonitor::~BoundListenerWithMonitor()
{
}


////////// OpaqueServer //////////

OpaqueServer::OpaqueServer(Handler& handler,
                           Event::Loop& eventLoop,
                           uint32_t maxMessageLength)
    : rpcHandler(handler)
    , eventLoop(eventLoop)
    , maxMessageLength(maxMessageLength)
    , sockets()
    , boundListenersMutex()
    , boundListeners()
{
}

OpaqueServer::~OpaqueServer()
{
    // Stop accepting new connections.
    {
        std::lock_guard<Core::Mutex> lock(boundListenersMutex);
        boundListeners.clear();
    }

    // Stop the socket objects from handling new RPCs and accessing the
    // 'sockets' set. They may continue to process existing RPCs, though
    // idle sockets will be destroyed here.
    {
        // Block the event loop to operate on 'sockets' safely.
        Event::Loop::Lock lockGuard(eventLoop);
        for (auto it = sockets.begin(); it != sockets.end(); ++it) {
            std::shared_ptr<SocketWithHandler> socket = *it;
            socket->handler.server = NULL;
        }
        sockets.clear();
    }
}

std::string
OpaqueServer::bind(const Address& listenAddress)
{
    using Core::StringUtil::format;

    if (!listenAddress.isValid()) {
        return format("Can't listen on invalid address: %s",
                      listenAddress.toString().c_str());
    }

    int fd = socket(AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0);
    if (fd < 0)
        PANIC("Could not create new TCP socket");

    int flag = 1;
    int r = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                       &flag, sizeof(flag));
    if (r < 0) {
        PANIC("Could not set SO_REUSEADDR on socket: %s",
              strerror(errno));
    }


    r = ::bind(fd, listenAddress.getSockAddr(),
                   listenAddress.getSockAddrLen());
    if (r != 0) {
        std::string msg =
            format("Could not bind to address %s: %s%s",
                   listenAddress.toString().c_str(),
                   strerror(errno),
                   errno == EINVAL ? " (is the port in use?)" : "");
        r = close(fd);
        if (r != 0) {
            WARNING("Could not close socket that failed to bind: %s",
                    strerror(errno));
        }
        return msg;
    }

    // Why 128? No clue. It's what libevent was setting it to.
    r = listen(fd, 128);
    if (r != 0) {
        PANIC("Could not invoke listen() on address %s: %s",
              listenAddress.toString().c_str(),
              strerror(errno));
    }

    std::lock_guard<Core::Mutex> lock(boundListenersMutex);
    boundListeners.emplace_back(*this, fd);
    return "";
}

} // namespace LogCabin::RPC
} // namespace LogCabin
