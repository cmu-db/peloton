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
#include "RPC/Address.h"

namespace LogCabin {
namespace RPC {
namespace {

typedef Address::TimePoint TimePoint;

TEST(RPCAddressTest, constructor) {
    Address blank("", 90);
    blank.refresh(TimePoint::max());
    EXPECT_EQ("No address given",
              blank.toString());

    // hostname
    Address name("example.com", 80);
    name.refresh(TimePoint::max());
    EXPECT_EQ("example.com", name.hosts.at(0).first);
    EXPECT_EQ("80", name.hosts.at(0).second);
    EXPECT_EQ("example.com", name.originalString);
    Address namePort("example.com:80", 90);
    namePort.refresh(TimePoint::max());
    EXPECT_EQ("example.com", namePort.hosts.at(0).first);
    EXPECT_EQ("80", namePort.hosts.at(0).second);
    EXPECT_EQ("example.com:80", namePort.originalString);

    // IPv4
    Address ipv4("1.2.3.4", 80);
    ipv4.refresh(TimePoint::max());
    EXPECT_EQ("1.2.3.4", ipv4.hosts.at(0).first);
    EXPECT_EQ("80", ipv4.hosts.at(0).second);
    EXPECT_EQ("1.2.3.4", ipv4.originalString);
    Address ipv4Port("1.2.3.4:80", 90);
    ipv4Port.refresh(TimePoint::max());
    EXPECT_EQ("1.2.3.4", ipv4Port.hosts.at(0).first);
    EXPECT_EQ("80", ipv4Port.hosts.at(0).second);
    EXPECT_EQ("1.2.3.4:80", ipv4Port.originalString);

    // IPv6
    Address ipv6("[1:2:3:4:5:6:7:8]", 80);
    ipv6.refresh(TimePoint::max());
    EXPECT_EQ("1:2:3:4:5:6:7:8", ipv6.hosts.at(0).first);
    EXPECT_EQ("80", ipv6.hosts.at(0).second);
    EXPECT_EQ("[1:2:3:4:5:6:7:8]", ipv6.originalString);
    Address ipv6Port("[1:2:3:4:5:6:7:8]:80", 90);
    ipv6Port.refresh(TimePoint::max());
    EXPECT_EQ("1:2:3:4:5:6:7:8", ipv6Port.hosts.at(0).first);
    EXPECT_EQ("80", ipv6Port.hosts.at(0).second);
    EXPECT_EQ("[1:2:3:4:5:6:7:8]:80", ipv6Port.originalString);
    Address ipv6Short("[::1]", 80);
    ipv6Short.refresh(TimePoint::max());
    EXPECT_EQ("::1", ipv6Short.hosts.at(0).first);
    EXPECT_EQ("80", ipv6Short.hosts.at(0).second);
    EXPECT_EQ("[::1]", ipv6Short.originalString);

    // multiple hosts
    Address all("example.com,"
                "example.com:80,"
                "1.2.3.4,"
                "1.2.3.4:80,"
                "[1:2:3:4:5:6:7:8],"
                "[1:2:3:4:5:6:7:8]:80,"
                "[::1]", 80);
    all.refresh(TimePoint::max());
    EXPECT_EQ((std::vector<std::pair<std::string, std::string>> {
                {"example.com", "80"},
                {"example.com", "80"},
                {"1.2.3.4", "80"},
                {"1.2.3.4", "80"},
                {"1:2:3:4:5:6:7:8", "80"},
                {"1:2:3:4:5:6:7:8", "80"},
                {"::1", "80"},
               }),
              all.hosts);

    Address commas(",,,example.com,,,,", 80);
    commas.refresh(TimePoint::max());
    EXPECT_EQ((std::vector<std::pair<std::string, std::string>> {
                {"example.com", "80"},
               }),
              commas.hosts);
}

TEST(RPCAddressTest, constructor_copy) {
    Address a("127.0.0.1", 80);
    a.refresh(TimePoint::max());
    Address b(a);
    EXPECT_EQ(a.hosts, b.hosts);
    EXPECT_EQ(a.len, b.len);
    EXPECT_EQ(a.toString(), b.toString());
    EXPECT_EQ(a.getResolvedString(), b.getResolvedString());
}

TEST(RPCAddressTest, assignment) {
    Address a("127.0.0.1", 80);
    a.refresh(TimePoint::max());
    Address b("127.0.0.2", 81);
    b = a;
    EXPECT_EQ(a.hosts, b.hosts);
    EXPECT_EQ(a.len, b.len);
    EXPECT_EQ(a.toString(), b.toString());
    EXPECT_EQ(a.getResolvedString(), b.getResolvedString());
}

TEST(RPCAddressTest, isValid) {
    Address a("127.0.0.1", 80);
    a.refresh(TimePoint::max());
    Address b("qqq", 81);
    b.refresh(TimePoint::max());
    EXPECT_TRUE(a.isValid());
    EXPECT_FALSE(b.isValid());
}

TEST(RPCAddressTest, getResolvedString) {
    // getResolvedString is tested adequately in the refresh test.
}

TEST(RPCAddressTest, toString) {
    EXPECT_EQ("No address given",
              Address().toString());
    Address a("127.0.0.1:80", 90);
    a.refresh(TimePoint::max());
    a.originalString = "example.org:80";
    EXPECT_EQ("example.org:80 (resolved to 127.0.0.1:80)",
              a.toString());
}

TEST(RPCAddressTest, refresh) {
    Address empty("", 80);
    empty.refresh(Address::TimePoint::max());
    EXPECT_FALSE(empty.isValid());

    // should be random, but should eventually refresh to all addresses
    Address multi("1.2.3.4,5.6.7.8", 80);
    std::set<std::string> resolved;
    for (uint64_t i = 0; i < 20; ++i) {
        multi.refresh(Address::TimePoint::max());
        resolved.insert(multi.getResolvedString());
    }
    EXPECT_EQ(2U, resolved.size());

    // This should be a pretty stable IP address, since it is supposed to be
    // easy to be memorize (at least for IPv4).
    Address google("google-public-dns-a.google.com", 80);
    google.refresh(TimePoint::max());
    std::string googleDNS = google.getResolvedString();
    if (googleDNS != "[2001:4860:4860::8888]:80") {
        EXPECT_EQ("8.8.8.8:80", googleDNS)
            << "This test requires connectivity to the Internet for a DNS "
            << "lookup. Alternatively, you can point "
            << "google-public-dns-a.google.com to 8.8.8.8 "
            << "in your /etc/hosts file.";
    }

    // IPv4
    Address ipv4a("1.2.3.4", 80);
    ipv4a.refresh(TimePoint::max());
    EXPECT_EQ("1.2.3.4:80", ipv4a.getResolvedString());
    Address ipv4b("0", 80);
    ipv4b.refresh(TimePoint::max());
    EXPECT_EQ("0.0.0.0:80", ipv4b.getResolvedString()) << "any address";

    // IPv6
    const char* disclaimer = "Failure of this test is normal if no external "
                             "network interface has an IPv6 address set.";
    Address ipv6a("[1:2:3:4:5:6:7:8]", 80);
    ipv6a.refresh(TimePoint::max());
    EXPECT_EQ("[1:2:3:4:5:6:7:8]:80",
              ipv6a.getResolvedString())
              << "random IPv6 address. " << disclaimer;
    Address ipv6b("[::1]", 80);
    ipv6b.refresh(TimePoint::max());
    EXPECT_EQ("[::1]:80",
              ipv6b.getResolvedString())
              << "localhost. " << disclaimer;
    Address ipv6c("[::]", 80);
    ipv6c.refresh(TimePoint::max());
    EXPECT_EQ("[::]:80",
              ipv6c.getResolvedString())
              << "any address. " << disclaimer;
}

} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
