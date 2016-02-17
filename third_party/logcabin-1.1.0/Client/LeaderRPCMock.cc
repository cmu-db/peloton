/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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

#include "Core/Debug.h"
#include "Client/LeaderRPCMock.h"
#include "Core/ProtoBuf.h"

namespace LogCabin {
namespace Client {

LeaderRPCMock::LeaderRPCMock()
    : requestLog()
    , responseQueue()
{
}

LeaderRPCMock::~LeaderRPCMock()
{
}

void
LeaderRPCMock::expect(OpCode opCode,
                      const google::protobuf::Message& response)
{
    MessagePtr responseCopy(response.New());
    responseCopy->CopyFrom(response);
    responseQueue.push({opCode, std::move(responseCopy)});
}

LeaderRPCMock::MessagePtr
LeaderRPCMock::popRequest()
{
    MessagePtr request = std::move(requestLog.at(0).second);
    requestLog.pop_front();
    return std::move(request);
}

LeaderRPCMock::Status
LeaderRPCMock::call(OpCode opCode,
          const google::protobuf::Message& request,
          google::protobuf::Message& response,
          TimePoint timeout)
{
    if (timeout < Clock::now())
        return Status::TIMEOUT;
    Call c(*this);
    c.start(opCode, request, timeout);
    c.wait(response, timeout);
    return Status::OK;
}

LeaderRPCMock::Call::Call(LeaderRPCMock& leaderRPC)
    : leaderRPC(leaderRPC)
    , canceled(false)
{
}

void
LeaderRPCMock::Call::start(OpCode opCode,
                           const google::protobuf::Message& request,
                           TimePoint timeout)
{
    MessagePtr requestCopy(request.New());
    requestCopy->CopyFrom(request);
    leaderRPC.requestLog.push_back({opCode, std::move(requestCopy)});
    EXPECT_LT(0U, leaderRPC.responseQueue.size())
        << "The client sent an unexpected RPC:\n"
        << request.GetTypeName() << ":\n"
        << Core::ProtoBuf::dumpString(request);
    if (leaderRPC.responseQueue.empty())
        throw std::runtime_error("unexpected RPC");
    auto& opCodeMsgPair = leaderRPC.responseQueue.front();
    EXPECT_EQ(opCode, opCodeMsgPair.first);
}

void
LeaderRPCMock::Call::cancel()
{
    canceled = true;
}

LeaderRPCMock::Call::Status
LeaderRPCMock::Call::wait(google::protobuf::Message& response,
                          TimePoint timeout)
{
    if (canceled) {
        leaderRPC.responseQueue.pop();
        return Status::RETRY;
    }
    auto& opCodeMsgPair = leaderRPC.responseQueue.front();
    response.CopyFrom(*opCodeMsgPair.second);
    leaderRPC.responseQueue.pop();
    return Status::OK;
}

std::unique_ptr<LeaderRPCBase::Call>
LeaderRPCMock::makeCall()
{
    return std::unique_ptr<LeaderRPCBase::Call>(new Call(*this));
}

} // namespace LogCabin::Client
} // namespace LogCabin
