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
#include "RPC/Server.h"
#include "RPC/ServerRPC.h"
#include "RPC/ThreadDispatchService.h"

namespace LogCabin {
namespace RPC {


////////// Server::RPCHandler //////////

Server::RPCHandler::RPCHandler(Server& server)
    : server(server)
{
}

Server::RPCHandler::~RPCHandler()
{
}

void
Server::RPCHandler::handleRPC(OpaqueServerRPC opaqueRPC)
{
    ServerRPC rpc(std::move(opaqueRPC));
    if (!rpc.needsReply()) {
        // The RPC may have had an invalid header, in which case it needs no
        // further action.
        return;
    }
    std::shared_ptr<Service> service;
    {
        std::lock_guard<std::mutex> lockGuard(server.mutex);
        auto it = server.services.find(rpc.getService());
        if (it != server.services.end())
            service = it->second;
    }
    if (service)
        service->handleRPC(std::move(rpc));
    else
        rpc.rejectInvalidService();
}

////////// Server //////////

Server::Server(Event::Loop& eventLoop, uint32_t maxMessageLength)
    : mutex()
    , services()
    , rpcHandler(*this)
    , opaqueServer(rpcHandler, eventLoop, maxMessageLength)
{
}

Server::~Server()
{
}

std::string
Server::bind(const Address& listenAddress)
{
    return opaqueServer.bind(listenAddress);
}

void
Server::registerService(uint16_t serviceId,
                        std::shared_ptr<Service> service,
                        uint32_t maxThreads)
{
    std::lock_guard<std::mutex> lockGuard(mutex);
    services[serviceId] =
        std::make_shared<ThreadDispatchService>(service, 0, maxThreads);
}

} // namespace LogCabin::RPC
} // namespace LogCabin
