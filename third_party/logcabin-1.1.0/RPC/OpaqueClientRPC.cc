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

#include "Core/Mutex.h"
#include "RPC/ClientSession.h"
#include "RPC/OpaqueClientRPC.h"

namespace LogCabin {
namespace RPC {

OpaqueClientRPC::OpaqueClientRPC()
    : mutex()
    , session()
    , responseToken(~0UL)
    , status(Status::NOT_READY)
    , reply()
    , errorMessage()
{
}

OpaqueClientRPC::OpaqueClientRPC(OpaqueClientRPC&& other)
    : mutex()
    , session(std::move(other.session))
    , responseToken(std::move(other.responseToken))
    , status(std::move(other.status))
    , reply(std::move(other.reply))
    , errorMessage(std::move(other.errorMessage))
{
}

OpaqueClientRPC::~OpaqueClientRPC()
{
    cancel();
}

OpaqueClientRPC&
OpaqueClientRPC::operator=(OpaqueClientRPC&& other)
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    session = std::move(other.session);
    responseToken = std::move(other.responseToken);
    status = std::move(other.status);
    reply = std::move(other.reply);
    errorMessage = std::move(other.errorMessage);
    return *this;
}

void
OpaqueClientRPC::cancel()
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    if (status != Status::NOT_READY)
        return;
    if (session)
        session->cancel(*this);
    status = Status::CANCELED;
    session.reset();
    reply.reset();
    errorMessage = "RPC canceled by user";
}

std::string
OpaqueClientRPC::getErrorMessage() const
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    const_cast<OpaqueClientRPC*>(this)->update();
    return errorMessage;
}

OpaqueClientRPC::Status
OpaqueClientRPC::getStatus() const
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    const_cast<OpaqueClientRPC*>(this)->update();
    return status;
}

Core::Buffer*
OpaqueClientRPC::peekReply()
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    update();
    if (status == Status::OK)
        return &reply;
    else
        return NULL;
}

void
OpaqueClientRPC::waitForReply(TimePoint timeout)
{
    std::unique_lock<std::mutex> mutexGuard(mutex);
    if (status != Status::NOT_READY)
        return;
    if (session) {
        {
            // release the mutex while calling wait()
            Core::MutexUnlock<std::mutex> unlockGuard(mutexGuard);
            session->wait(*this, timeout);
        }
        update();
    } else {
        errorMessage = "This RPC was never associated with a ClientSession.";
        status = Status::ERROR;
    }
}

///// private methods /////

void
OpaqueClientRPC::update()
{
    if (status == Status::NOT_READY && session)
        session->update(*this);
}

///// exported functions /////

::std::ostream&
operator<<(::std::ostream& os, OpaqueClientRPC::Status status)
{
    typedef OpaqueClientRPC::Status Status;
    switch (status) {
        case Status::NOT_READY:
            return os << "NOT_READY";
        case Status::OK:
            return os << "OK";
        case Status::ERROR:
            return os << "ERROR";
        case Status::CANCELED:
            return os << "CANCELED";
        default:
            return os << "(INVALID VALUE)";
    }
}

} // namespace LogCabin::RPC
} // namespace LogCabin
