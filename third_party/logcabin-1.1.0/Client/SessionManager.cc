/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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

#include "Client/SessionManager.h"
#include "Core/ProtoBuf.h"
#include "Protocol/Common.h"
#include "RPC/ClientRPC.h"
#include "RPC/ClientSession.h"
#include "build/Protocol/Client.pb.h"

namespace LogCabin {
namespace Client {

SessionManager::SessionManager(Event::Loop& eventLoop,
                               const Core::Config& config)
    : eventLoop(eventLoop)
    , config(config)
    , skipVerify(false)
{
}

std::shared_ptr<RPC::ClientSession>
SessionManager::createSession(const RPC::Address& address,
                              RPC::Address::TimePoint timeout,
                              ClusterUUID* clusterUUID,
                              ServerId* serverId)
{
    std::shared_ptr<RPC::ClientSession> session =
        RPC::ClientSession::makeSession(
                        eventLoop,
                        address,
                        Protocol::Common::MAX_MESSAGE_LENGTH,
                        timeout,
                        config);
    if (!session->getErrorMessage().empty() || skipVerify)
        return session;

    Protocol::Client::VerifyRecipient::Request request;
    if (clusterUUID != NULL) {
        std::string uuid = clusterUUID->getOrDefault();
        if (!uuid.empty())
            request.set_cluster_uuid(uuid);
    }
    if (serverId != NULL) {
        std::pair<bool, uint64_t> c = serverId->get();
        if (c.first)
            request.set_server_id(c.second);
    }

    RPC::ClientRPC rpc(session,
                       Protocol::Common::ServiceId::CLIENT_SERVICE,
                       1,
                       Protocol::Client::OpCode::VERIFY_RECIPIENT,
                       request);

    typedef RPC::ClientRPC::Status RPCStatus;
    Protocol::Client::VerifyRecipient::Response response;
    Protocol::Client::Error error;
    RPCStatus status = rpc.waitForReply(&response, &error, timeout);

    // Decode the response
    switch (status) {
        case RPCStatus::OK:
            if (response.ok()) {
                if (!request.has_cluster_uuid() &&
                    response.has_cluster_uuid() &&
                    !response.cluster_uuid().empty() &&
                    clusterUUID != NULL) {
                    clusterUUID->set(response.cluster_uuid());
                }
                if (!request.has_server_id() &&
                    response.has_server_id() &&
                    serverId != NULL) {
                    serverId->set(response.server_id());
                }
                return session;
            } else {
                ERROR("Intended recipient was not at %s: %s. "
                      "Closing session.",
                      session->toString().c_str(),
                      response.error().c_str());
                break;
            }
        case RPCStatus::RPC_FAILED:
            break;
        case RPCStatus::TIMEOUT:
            break;
        case RPCStatus::SERVICE_SPECIFIC_ERROR:
            // Hmm, we don't know what this server is trying to tell us,
            // but something is wrong. The server shouldn't reply back with
            // error codes we don't understand. That's why we gave it a
            // serverSpecificErrorVersion number in the request header.
            PANIC("Unknown error code %u returned in service-specific "
                  "error. This probably indicates a bug in the server",
                  error.error_code());
            break;
        case RPCStatus::RPC_CANCELED:
            PANIC("RPC canceled unexpectedly");
        case RPCStatus::INVALID_SERVICE:
            PANIC("The server isn't running the ClientService");
        case RPCStatus::INVALID_REQUEST:
            PANIC("The server's ClientService doesn't support the "
                  "VerifyRecipient RPC or claims the request is malformed");
    }
    return RPC::ClientSession::makeErrorSession(
        eventLoop,
        Core::StringUtil::format("Verifying recipient with %s failed "
                                 "(after connecting over TCP)",
                                 address.toString().c_str()));
}

} // namespace LogCabin::Client
} // namespace LogCabin
