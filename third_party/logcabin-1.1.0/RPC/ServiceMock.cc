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

#include <gtest/gtest.h>

#include "Core/ProtoBuf.h"
#include "RPC/ServiceMock.h"

namespace LogCabin {
namespace RPC {

ServiceMock::Expected::Expected(uint16_t opCode,
                 std::unique_ptr<Message> request,
                 std::shared_ptr<Handler> response)
    : opCode(opCode)
    , request(std::move(request))
    , response(response)
{
}

ServiceMock::Expected::Expected(ServiceMock::Expected&& other)
    : opCode(other.opCode)
    , request(std::move(other.request))
    , response(other.response)
{
}

ServiceMock::Expected&
ServiceMock::Expected::operator=(ServiceMock::Expected&& other)
{
    opCode = other.opCode;
    request = std::move(other.request);
    response = other.response;
    return *this;
}

void
ServiceMock::handleRPC(RPC::ServerRPC rpc)
{
    if (responseQueue.empty()) {
        rpc.rejectInvalidRequest();
        return;
    }
    Expected& exp = responseQueue.front();
    EXPECT_EQ(exp.opCode, rpc.getOpCode());
    if (rpc.getOpCode() != exp.opCode) {
        rpc.rejectInvalidRequest();
        return;
    }
    std::unique_ptr<Message> actualReq(exp.request->New());
    bool b = rpc.getRequest(*actualReq);
    EXPECT_TRUE(b) << "rpc.getRequest() failed";
    if (!b)
        return;
    EXPECT_EQ(*exp.request, *actualReq);
    if (*exp.request != *actualReq) {
        rpc.rejectInvalidRequest();
        return;
    }
    exp.response->handleRPC(std::move(rpc));
    responseQueue.pop();
}

std::string
ServiceMock::getName() const
{
    return "MockService";
}

void
ServiceMock::expect(uint16_t opCode,
                    const Message& request,
                    std::shared_ptr<Handler> response)
{
    responseQueue.emplace(opCode, Core::ProtoBuf::copy(request), response);
}

} // namespace LogCabin::RPC
} // namespace LogCabin
