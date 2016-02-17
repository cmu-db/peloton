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

#include "Core/Endian.h"
#include "RPC/Protocol.h"

namespace LogCabin {
namespace RPC {
namespace Protocol {

void
RequestHeaderPrefix::fromBigEndian()
{
    // version is only 1 byte, nothing to flip
}

void
RequestHeaderPrefix::toBigEndian()
{
    // version is only 1 byte, nothing to flip
}

void
RequestHeaderVersion1::fromBigEndian()
{
    service = be16toh(service);
    // serviceSpecificErrorVersion is only 1 byte, nothing to flip
    opCode = be16toh(opCode);
}

void
RequestHeaderVersion1::toBigEndian()
{
    service = htobe16(service);
    // serviceSpecificErrorVersion is only 1 byte, nothing to flip
    opCode = htobe16(opCode);
}

::std::ostream&
operator<<(::std::ostream& stream, Status status)
{
    switch (status) {
        case Status::OK:
            stream << "OK";
            break;
        case Status::SERVICE_SPECIFIC_ERROR:
            stream << "SERVICE_SPECIFIC_ERROR";
            break;
        case Status::INVALID_VERSION:
            stream << "INVALID_VERSION";
            break;
        case Status::INVALID_SERVICE:
            stream << "INVALID_SERVICE";
            break;
        case Status::INVALID_REQUEST:
            stream << "INVALID_REQUEST";
            break;
        default:
            stream << "(bad Status value " << uint8_t(status) << " )";
            break;
    }
    return stream;
}

void
ResponseHeaderPrefix::fromBigEndian()
{
    // status is only 1 byte, nothing to flip
}

void
ResponseHeaderPrefix::toBigEndian()
{
    // status is only 1 byte, nothing to flip
}

void
ResponseHeaderVersion1::fromBigEndian()
{
    // no fields, nothing to flip
}

void
ResponseHeaderVersion1::toBigEndian()
{
    // no fields, nothing to flip
}

} // namespace LogCabin::RPC::Protocol
} // namespace LogCabin::RPC
} // namespace LogCabin
