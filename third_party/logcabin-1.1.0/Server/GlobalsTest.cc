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

#include "Protocol/Common.h"
#include "RPC/Address.h"
#include "RPC/Server.h"
#include "Server/Globals.h"

namespace LogCabin {
namespace Server {
namespace {

TEST(ServerGlobalsTest, basics) {
    Globals globals;
    globals.config.set("storageModule", "Memory");
    globals.config.set("uuid", "my-fake-uuid-123");
    globals.config.set("listenAddresses", "127.0.0.1");
    globals.config.set("serverId", "1");
    globals.config.set("use-temporary-storage", "true");
    globals.init();
    globals.eventLoop.exit();
    globals.run();
}

TEST(ServerGlobalsTest, initNoServers) {
    Globals globals;
    globals.config.set("storageModule", "Memory");
    globals.config.set("uuid", "my-fake-uuid-123");
    globals.config.set("use-temporary-storage", "true");
    globals.config.set("serverId", "1");
    globals.config.set("listenAddresses", "");
    EXPECT_DEATH(globals.init(),
                 "No server addresses specified");
}

TEST(ServerGlobalsTest, initEmptyServers) {
    Globals globals;
    globals.config.set("storageModule", "Memory");
    globals.config.set("uuid", "my-fake-uuid-123");
    globals.config.set("listenAddresses", ",");
    globals.config.set("serverId", "1");
    globals.config.set("use-temporary-storage", "true");
    EXPECT_DEATH(globals.init(),
                 "invalid address");
}

TEST(ServerGlobalsTest, initAddressTaken) {
    Event::Loop eventLoop;
    RPC::Server server(eventLoop, 1);
    RPC::Address address("127.0.0.1",
                         Protocol::Common::DEFAULT_PORT);
    address.refresh(RPC::Address::TimePoint::max());
    EXPECT_EQ("", server.bind(address));

    Globals globals;
    globals.config.set("storageModule", "Memory");
    globals.config.set("uuid", "my-fake-uuid-123");
    globals.config.set("listenAddresses", "127.0.0.1");
    globals.config.set("serverId", "1");
    globals.config.set("use-temporary-storage", "true");
    EXPECT_DEATH(globals.init(),
                 "in use");
}

TEST(ServerGlobalsTest, initBindToAll) {
    Globals globals;
    globals.config.set("storageModule", "Memory");
    globals.config.set("uuid", "my-fake-uuid-123");
    globals.config.set("listenAddresses", "127.0.0.1:5254,127.0.0.1:5255");
    globals.config.set("serverId", "1");
    globals.config.set("use-temporary-storage", "true");
    globals.init();
    Event::Loop eventLoop;
    RPC::Server server(eventLoop, 1);
    RPC::Address address("127.0.0.1", 5255);
    address.refresh(RPC::Address::TimePoint::max());
    std::string e = server.bind(address);
    EXPECT_NE(e.npos, e.find("in use")) << e;
}

} // namespace LogCabin::Server::<anonymous>
} // namespace LogCabin::Server
} // namespace LogCabin
