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

#include <cinttypes>
#include <google/protobuf/message.h>
#include <memory>
#include <queue>

#include "Core/ProtoBuf.h"
#include "RPC/Service.h"
#include "RPC/ServerRPC.h"

#ifndef LOGCABIN_RPC_SERVICEMOCK_H
#define LOGCABIN_RPC_SERVICEMOCK_H

namespace LogCabin {
namespace RPC {

/**
 * This class is used to mock out a Service for testing a client that sends
 * RPCs.
 */
class ServiceMock : public RPC::Service {
  public:
    typedef google::protobuf::Message Message;

    /**
     * The base class for RPC handlers. These are called when the matching
     * request arrives and act on the request, for example, by replying to it.
     */
    class Handler {
        virtual ~Handler() {}
        virtual void handleRPC(ServerRPC serverRPC) = 0;
    };

  private:
    /// See closeSession().
    class CloseSession : public Handler {
        void handleRPC(ServerRPC serverRPC) {
            serverRPC.closeSession();
        }
    };

    /// See reply().
    class Reply : public Handler {
        explicit Reply(const Message& response)
            : response(Core::ProtoBuf::copy(response)) {
        }
        void handleRPC(ServerRPC serverRPC) {
            serverRPC.reply(*response);
        }
        std::unique_ptr<Message> response;
    };

    /// See serviceSpecificError().
    class ServiceSpecificError : public Handler {
        explicit ServiceSpecificError(const Message& response)
            : response(Core::ProtoBuf::copy(response)) {
        }
        void handleRPC(ServerRPC serverRPC) {
            serverRPC.returnError(*response);
        }
        std::unique_ptr<Message> response;
    };

    /// See rejectInvalidRequest().
    class RejectInvalidRequest : public Handler {
        void handleRPC(ServerRPC serverRPC) {
            serverRPC.rejectInvalidRequest();
        }
    };

  public:

    /**
     * Constructor.
     */
    ServiceMock()
        : responseQueue()
    {
    }

    /**
     * Destructor. Makes sure no more requests are expected.
     */
    ~ServiceMock() {
        EXPECT_EQ(0U, responseQueue.size());
    }

    /**
     * Remove previously expected requests.
     */
    void clear() {
        responseQueue = std::queue<Expected>();
    }

    /**
     * Close the client's session when the specified request arrives (in FIFO
     * order).
     */
    void closeSession(uint16_t opCode, const Message& request) {
        expect(opCode, request, std::make_shared<CloseSession>());
    }

    /**
     * Reply normally to the client when the specified request arrives (in FIFO
     * order).
     */
    void reply(uint16_t opCode, const Message& request,
               const Message& response) {
        expect(opCode, request, std::make_shared<Reply>(response));
    }

    /**
     * Reply with a service-specific error to the client when the specified
     * request arrives (in FIFO order).
     */
    void serviceSpecificError(uint16_t opCode, const Message& request,
                              const Message& response) {
        expect(opCode, request,
               std::make_shared<ServiceSpecificError>(response));
    }

    /**
     * Reject the client's RPC as an invalid request when the specified request
     * arrives (in FIFO order).
     */
    void rejectInvalidRequest(uint16_t opCode, const Message& request) {
        expect(opCode, request, std::make_shared<RejectInvalidRequest>());
    }

    /**
     * Call a custom handler when the request arrives.
     */
    void runArbitraryCode(uint16_t opCode,
                          const Message& request,
                          std::shared_ptr<Handler> response) {
        expect(opCode, request, response);
    }

    /**
     * This is called by the Server.
     */
    void handleRPC(RPC::ServerRPC serverRPC);

    std::string getName() const;

  private:
    /**
     * Call the Handler when the specified request arrives (in FIFO order).
     */
    void expect(uint16_t opCode,
                const Message& request,
                std::shared_ptr<Handler> response);

    /// See #responseQueue.
    struct Expected {
        Expected(uint16_t opCode,
                 std::unique_ptr<Message> request,
                 std::shared_ptr<Handler> response);
        Expected(Expected&& other);
        Expected& operator=(Expected&& other);
        /// The op code expected from the client.
        uint16_t opCode;
        /// The request message expected from the client.
        std::unique_ptr<Message> request;
        /// Code to respond to the client or close its session, etc.
        std::shared_ptr<Handler> response;
    };

    /**
     * A FIFO queue of requests to expect from clients and their associated
     * handlers.
     */
    std::queue<Expected> responseQueue;
};

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_SERVICEMOCK_H */
