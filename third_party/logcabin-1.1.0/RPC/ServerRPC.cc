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

#include "Core/ProtoBuf.h"
#include "RPC/ServerRPC.h"

namespace LogCabin {
namespace RPC {

using RPC::Protocol::RequestHeaderPrefix;
using RPC::Protocol::RequestHeaderVersion1;
using RPC::Protocol::ResponseHeaderPrefix;
using RPC::Protocol::ResponseHeaderVersion1;
using RPC::Protocol::Status;

ServerRPC::ServerRPC(OpaqueServerRPC opaqueRPC)
    : opaqueRPC(std::move(opaqueRPC))
    , active(true)
    , service(0)
    , serviceSpecificErrorVersion(0)
    , opCode(0)
{
    const Core::Buffer& request = this->opaqueRPC.request;

    // Carefully read the headers.
    if (request.getLength() < sizeof(RequestHeaderPrefix)) {
        reject(Status::INVALID_REQUEST);
        return;
    }
    RequestHeaderPrefix requestHeaderPrefix =
        *static_cast<const RequestHeaderPrefix*>(request.getData());
    requestHeaderPrefix.fromBigEndian();
    if (requestHeaderPrefix.version != 1 ||
        request.getLength() < sizeof(RequestHeaderVersion1)) {
        reject(Status::INVALID_VERSION);
        return;
    }
    RequestHeaderVersion1 requestHeader =
        *static_cast<const RequestHeaderVersion1*>(request.getData());
    requestHeader.fromBigEndian();

    service = requestHeader.service;
    serviceSpecificErrorVersion = requestHeader.serviceSpecificErrorVersion;
    opCode = requestHeader.opCode;
}

ServerRPC::ServerRPC()
    : opaqueRPC()
    , active(false)
    , service(0)
    , serviceSpecificErrorVersion(0)
    , opCode(0)
{
}

ServerRPC::ServerRPC(ServerRPC&& other)
    : opaqueRPC(std::move(other.opaqueRPC))
    , active(other.active)
    , service(other.service)
    , serviceSpecificErrorVersion(other.serviceSpecificErrorVersion)
    , opCode(other.opCode)
{
    other.active = false;
}

ServerRPC::~ServerRPC()
{
    if (active) {
        WARNING("ServerRPC destroyed without a reply (service %u, opcode %u). "
                "This may cause the client of the RPC to hang",
                service, opCode);
    }
}

ServerRPC&
ServerRPC::operator=(ServerRPC&& other)
{
    opaqueRPC = std::move(other.opaqueRPC);
    active = other.active;
    other.active = false;
    service = other.service;
    serviceSpecificErrorVersion = other.serviceSpecificErrorVersion;
    opCode = other.opCode;
    return *this;
}

bool
ServerRPC::getRequest(google::protobuf::Message& request)
{
    if (!active)
        return false;
    if (!Core::ProtoBuf::parse(opaqueRPC.request, request,
                               sizeof(RequestHeaderVersion1))) {
        rejectInvalidRequest();
        return false;
    }
    return true;
}

bool
ServerRPC::getRequest(Core::Buffer& buffer) const
{
    if (!active)
        return false;
    uint64_t bytes = opaqueRPC.request.getLength();
    assert(bytes >= sizeof(RequestHeaderVersion1));
    bytes -= sizeof(RequestHeaderVersion1);
    buffer.setData(new char[bytes],
                   bytes,
                   Core::Buffer::deleteArrayFn<char>);
    memcpy(buffer.getData(),
           (static_cast<const char*>(opaqueRPC.request.getData()) +
            sizeof(RequestHeaderVersion1)),
           bytes);
    return true;
}

void
ServerRPC::reply(const google::protobuf::Message& payload)
{
    active = false;
    Core::Buffer buffer;
    Core::ProtoBuf::serialize(payload, buffer,
                              sizeof(ResponseHeaderVersion1));
    auto& responseHeader =
        *static_cast<ResponseHeaderVersion1*>(buffer.getData());
    responseHeader.prefix.status = Status::OK;
    responseHeader.prefix.toBigEndian();
    responseHeader.toBigEndian();
    opaqueRPC.response = std::move(buffer);
    opaqueRPC.sendReply();
}

void
ServerRPC::returnError(const google::protobuf::Message& serviceSpecificError)
{
    active = false;
    Core::Buffer buffer;
    Core::ProtoBuf::serialize(serviceSpecificError, buffer,
                              sizeof(ResponseHeaderVersion1));
    auto& responseHeader =
        *static_cast<ResponseHeaderVersion1*>(buffer.getData());
    responseHeader.prefix.status = Status::SERVICE_SPECIFIC_ERROR;
    responseHeader.prefix.toBigEndian();
    responseHeader.toBigEndian();
    opaqueRPC.response = std::move(buffer);
    opaqueRPC.sendReply();
}

void
ServerRPC::rejectInvalidService()
{
    reject(Status::INVALID_SERVICE);
}

void
ServerRPC::rejectInvalidRequest()
{
    reject(Status::INVALID_REQUEST);
}

void
ServerRPC::closeSession()
{
    active = false;
    opaqueRPC.closeSession();
}

void
ServerRPC::reject(RPC::Protocol::Status status)
{
    active = false;
    ResponseHeaderVersion1& responseHeader = *new ResponseHeaderVersion1();
    responseHeader.prefix.status = status;
    responseHeader.prefix.toBigEndian();
    responseHeader.toBigEndian();
    opaqueRPC.response.setData(
        &responseHeader,
        sizeof(responseHeader),
        Core::Buffer::deleteObjectFn<ResponseHeaderVersion1*>);
    opaqueRPC.sendReply();
}


} // namespace LogCabin::RPC
} // namespace LogCabin
