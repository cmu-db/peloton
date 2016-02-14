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
#include <memory>
#include <mutex>
#include <unordered_map>

#include "RPC/OpaqueServer.h"
#include "RPC/Service.h"

#ifndef LOGCABIN_RPC_SERVER_H
#define LOGCABIN_RPC_SERVER_H

namespace LogCabin {
namespace RPC {

/**
 * A Server listens for incoming RPCs over TCP connections and dispatches these
 * to Services.
 * Servers can be created from any thread, but they will always run on
 * the thread running the Event::Loop. Services will always run on a thread
 * pool.
 */
class Server {
  public:
    /**
     * Constructor. This object won't actually do anything until bind() is
     * called.
     * \param eventLoop
     *      Event::Loop that will be used to find out when the underlying
     *      socket may be read from or written to without blocking.
     * \param maxMessageLength
     *      The maximum number of bytes to allow per request/response. This
     *      exists to limit the amount of buffer space a single RPC can use.
     *      Attempting to send longer responses will PANIC; attempting to
     *      receive longer requests will disconnect the underlying socket.
     */
    Server(Event::Loop& eventLoop, uint32_t maxMessageLength);

    /**
     * Destructor. ServerRPC objects originating from this Server may be kept
     * around after this destructor returns; however, they won't actually send
     * replies anymore.
     */
    ~Server();

    /**
     * See OpaqueServer::bind().
     */
    std::string bind(const Address& listenAddress);

    /**
     * Register a Service to receive RPCs from clients. If a service has
     * already been registered for this service ID, this will replace it. This
     * may be called from any thread.
     * \param serviceId
     *      A unique ID for the service. See Protocol::Common::ServiceId.
     * \param service
     *      The service to invoke when RPCs arrive with the given serviceId.
     *      This service will always be invoked on a thread pool.
     * \param maxThreads
     *      The maximum number of threads to execute RPCs concurrently inside
     *      the service.
     */
    void registerService(uint16_t serviceId,
                         std::shared_ptr<Service> service,
                         uint32_t maxThreads);

  private:
    /**
     * Services RPCs.
     */
    class RPCHandler : public OpaqueServer::Handler {
      public:
        explicit RPCHandler(Server& server);
        ~RPCHandler();
        /**
         * This is called by the base class, OpaqueServer::Handler, when an RPC
         * arrives.
         */
        void handleRPC(OpaqueServerRPC opaqueRPC);
        Server& server;
    };

    /**
     * Protects #services from concurrent modification.
     */
    std::mutex mutex;

    /**
     * Maps from service IDs to ThreadDispatchService instances.
     * Protected by #mutex.
     */
    std::unordered_map<uint16_t, std::shared_ptr<Service>> services;

    /**
     * Deals with RPCs created by #opaqueServer.
     */
    RPCHandler rpcHandler;

    /**
     * Listens for new RPCs on TCP connections and invokes #rpcHandler with
     * them.
     */
    OpaqueServer opaqueServer;

    // Server is non-copyable.
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
}; // class Server

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_SERVER_H */
