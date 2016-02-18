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

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/ip.h>
#include <string.h>
#include <sys/types.h>

#include <sstream>
#include <vector>

#include "Core/Debug.h"
#include "Core/Endian.h"
#include "Core/Random.h"
#include "Core/StringUtil.h"
#include "RPC/Address.h"

namespace LogCabin {
namespace RPC {

Address::Address(const std::string& str, uint16_t defaultPort)
    : originalString(str)
    , hosts()
    , storage()
    , len(0)
{
    memset(&storage, 0, sizeof(storage));

    std::vector<std::string> hostsList = Core::StringUtil::split(str, ',');
    for (auto it = hostsList.begin(); it != hostsList.end(); ++it) {
        std::string host = *it;
        std::string port;
        if (host.empty())
            continue;

        size_t lastColon = host.rfind(':');
        if (lastColon != host.npos &&
            host.find(']', lastColon) == host.npos) {
            // following lastColon is a port number
            port = host.substr(lastColon + 1);
            host.erase(lastColon);
        } else {
            // use default port
            port = Core::StringUtil::toString(defaultPort);
        }

        // IPv6 hosts are surrounded in brackets. These need to be stripped.
        if (host.at(0) == '[' &&
            host.at(host.length() - 1) == ']') {
            host = host.substr(1, host.length() - 2);
        }

        hosts.push_back({host, port});
    }
}

Address::Address()
    : originalString("")
    , hosts()
    , storage()
    , len(0)
{
}

Address::Address(const Address& other)
    : originalString(other.originalString)
    , hosts(other.hosts)
    , storage()
    , len(other.len)
{
    memcpy(&storage, &other.storage, sizeof(storage));
}

Address&
Address::operator=(const Address& other)
{
    originalString = other.originalString;
    hosts = other.hosts;
    memcpy(&storage, &other.storage, sizeof(storage));
    len = other.len;
    return *this;
}

bool
Address::isValid() const
{
    return len > 0;
}

const sockaddr*
Address::getSockAddr() const
{
    return reinterpret_cast<const sockaddr*>(&storage);
}

socklen_t
Address::getSockAddrLen() const
{
    return len;
}

std::string
Address::getResolvedString() const
{
    std::stringstream ret;
    switch (getSockAddr()->sa_family) {
        case AF_UNSPEC:
            return "Unspecified";
        case AF_INET: {
            const sockaddr_in* addr =
                reinterpret_cast<const sockaddr_in*>(getSockAddr());
            char ipBuf[INET_ADDRSTRLEN];
            ret << inet_ntop(AF_INET, &addr->sin_addr,
                             ipBuf, sizeof(ipBuf));
            ret << ":";
            ret << be16toh(addr->sin_port);
            break;
        }
        case AF_INET6: {
            const sockaddr_in6* addr =
                reinterpret_cast<const sockaddr_in6*>(getSockAddr());
            char ipBuf[INET6_ADDRSTRLEN];
            ret << "[";
            ret << inet_ntop(AF_INET6, &addr->sin6_addr,
                             ipBuf, sizeof(ipBuf));
            ret << "]:";
            ret << be16toh(addr->sin6_port);
            break;
        }
        default:
            return "Unknown protocol";
    }
    return ret.str();
}

std::string
Address::toString() const
{
    if (originalString.empty()) {
        return "No address given";
    } else {
        return Core::StringUtil::format(
                    "%s (resolved to %s)",
                    originalString.c_str(),
                    getResolvedString().c_str());
    }
}

void
Address::refresh(TimePoint timeout)
{
    if (hosts.empty())
        return;
    size_t hostIdx = Core::Random::random32() % hosts.size();
    const std::string& host = hosts.at(hostIdx).first;
    const std::string& port = hosts.at(hostIdx).second;
    VERBOSE("Running getaddrinfo for host %s with port %s",
            host.c_str(), port.c_str());

    addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_flags = AI_NUMERICSERV | AI_V4MAPPED | AI_ADDRCONFIG;

    addrinfo* result = NULL;
    int r = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
    switch (r) {
        // Success.
        case 0: {
            // Look for IPv4 and IPv6 addresses.
            std::vector<addrinfo*> candidates;
            for (addrinfo* addr = result; addr != NULL; addr = addr->ai_next) {
                if (addr->ai_family == AF_INET ||
                    addr->ai_family == AF_INET6) {
                   candidates.push_back(addr);
                }
            }
            if (!candidates.empty()) {
                // Select one randomly and hope it works.
                size_t idx = Core::Random::random32() % candidates.size();
                addrinfo* addr = candidates.at(idx);
                memcpy(&storage, addr->ai_addr, addr->ai_addrlen);
                len = addr->ai_addrlen;
            }
            break;
        }

        // These are somewhat normal errors.
        case EAI_FAMILY:
            break;
        case EAI_NONAME:
            break;
        case EAI_NODATA:
            break;

        // This is unexpected.
        default:
            WARNING("Unknown error from getaddrinfo(\"%s\", \"%s\"): %s",
                    host.c_str(), port.c_str(), gai_strerror(r));
    }
    if (result != NULL)
        freeaddrinfo(result);

    VERBOSE("Result: %s", toString().c_str());
}


} // namespace LogCabin::RPC
} // namespace LogCabin
