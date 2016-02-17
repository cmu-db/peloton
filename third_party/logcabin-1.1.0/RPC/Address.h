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

#ifndef LOGCABIN_RPC_ADDRESS_H
#define LOGCABIN_RPC_ADDRESS_H

#include <sys/socket.h>
#include <string>
#include <vector>

#include "Core/Time.h"

namespace LogCabin {
namespace RPC {

/**
 * This class resolves user-friendly addresses for services into socket-level
 * addresses. It supports DNS lookups for addressing hosts by name, and it
 * supports multiple (alternative) addresses.
 */
class Address {
  public:
    /// Clock used for timeouts.
    typedef Core::Time::SteadyClock Clock;
    /// Type for absolute time values used for timeouts.
    typedef Clock::time_point TimePoint;

    /**
     * Constructor. You will usually need to call #refresh() before using this
     * class.
     * \param str
     *      A string representation of the host and, optionally, a port number.
     *          - hostname:port
     *          - hostname
     *          - IPv4Address:port
     *          - IPv4Address
     *          - [IPv6Address]:port
     *          - [IPv6Address]
     *      Or a comma-delimited list of these to represent multiple hosts.
     * \param defaultPort
     *      The port number to use if none is specified in str.
     */
    Address(const std::string& str, uint16_t defaultPort);

    /// Default constructor.
    Address();

    /// Copy constructor.
    Address(const Address& other);

    /// Assignment.
    Address& operator=(const Address& other);

    /**
     * Return true if the sockaddr returned by getSockAddr() is valid.
     * \return
     *      True if refresh() has ever succeeded for this host and port.
     *      False otherwise.
     */
    bool isValid() const;

    /**
     * Return a sockaddr that may be used to connect a socket to this Address.
     * \return
     *      The returned value will never be NULL and it is always safe to read
     *      the protocol field from it, even if getSockAddrLen() returns 0.
     */
    const sockaddr* getSockAddr() const;

    /**
     * Return the length in bytes of the sockaddr in getSockAddr().
     * This is the value you'll want to pass in to connect() or bind().
     */
    socklen_t getSockAddrLen() const;

    /**
     * Return a string describing the sockaddr within this Address.
     * This string will reflect the numeric address produced by the latest
     * successful call to refresh(), or "Unspecified".
     */
    std::string getResolvedString() const;

    /**
     * Return a string describing this Address.
     * This will contain both the user-provided string passed into the
     * constructor and the numeric address produced by the latest successful
     * call to refresh(). It's the best representation to use in error messages
     * for the user.
     */
    std::string toString() const;

    /**
     * Convert (a random one of) the host(s) and port(s) to a sockaddr.
     * If the host is a name instead of numeric, this will run a DNS query and
     * select a random result. If this query fails, any previous sockaddr will
     * be left intact.
     * \param timeout
     *      Not yet implemented.
     * \warning
     *      Timeouts have not been implemented.
     *      See https://github.com/logcabin/logcabin/issues/75
     */
    void refresh(TimePoint timeout);

  private:

    /**
     * The host name(s) or numeric address(es) as passed into the constructor.
     */
    std::string originalString;

    /**
     * A list of (host, port) pairs as parsed from originalString.
     * - First component: the host name or numeric address as parsed from the
     *   string passed into the constructor. This has brackets stripped out of
     *   IPv6 addresses and is in the form needed by getaddrinfo().
     * - Second component: an ASCII representation of the port number to use.
     *   It is stored in string form because that's sometimes how it comes into
     *   the constructor and always what refresh() needs to call getaddrinfo().
     */
    std::vector<std::pair<std::string, std::string>> hosts;

    /**
     * Storage for the sockaddr returned by getSockAddr.
     * This is always zeroed out from len to the end.
     */
    sockaddr_storage storage;

    /**
     * The length in bytes of storage that are in use.
     * The remaining bytes of storage are always zeroed out.
     */
    socklen_t len;
};

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_ADDRESS_H */
