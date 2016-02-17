/* Copyright (c) 2012 Stanford University
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

#include <gtest/gtest.h>
#include <deque>
#include <queue>

#include "Client/ClientImpl.h"
#include "Client/LeaderRPCMock.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/StringUtil.h"
#include "build/Protocol/Client.pb.h"
#include "include/LogCabin/Client.h"

namespace LogCabin {
namespace {

class ClientMockClientImplTest : public ::testing::Test {
  public:
    ClientMockClientImplTest()
        : cluster(new Client::Cluster(
            std::make_shared<Client::TestingCallbacks>()))
    {
    }
    std::unique_ptr<Client::Cluster> cluster;
};

// sanity check for tree operations (read-only and read-write)
TEST_F(ClientMockClientImplTest, tree) {
    Client::Tree tree = cluster->getTree();
    EXPECT_EQ(Client::Status::OK,
              tree.makeDirectory("/foo").status);
    std::vector<std::string> children;
    EXPECT_EQ(Client::Status::OK,
              tree.listDirectory("/", children).status);
    EXPECT_EQ((std::vector<std::string> {"foo/"}),
              children);
}

class MyCallbacks : public Client::TestingCallbacks {
  public:
    MyCallbacks()
        : tree()
    {
    }
    bool stateMachineCommand(
        Protocol::Client::StateMachineCommand_Request& request,
        Protocol::Client::StateMachineCommand_Response& response) {
        if (request.has_tree()) {
            return readWriteTreeRPC(*request.mutable_tree(),
                                    *response.mutable_tree());
        }
        return false;
    }
    bool stateMachineQuery(
        Protocol::Client::StateMachineQuery_Request& request,
        Protocol::Client::StateMachineQuery_Response& response) {
        if (request.has_tree()) {
            return readOnlyTreeRPC(*request.mutable_tree(),
                                   *response.mutable_tree());
        }
        return false;
    }

    bool readOnlyTreeRPC(
        Protocol::Client::ReadOnlyTree_Request& request,
        Protocol::Client::ReadOnlyTree_Response& response) {
        if (request.has_read()) {
            if (request.read().path() == "/foo") {
                response.set_status(Protocol::Client::TIMEOUT);
                response.set_error("timed out");
                return true;
            } else if (request.read().path() == "/bar") {
                response.set_status(Protocol::Client::OK);
                response.mutable_read()->set_contents(tree->readEx("/foo"));
                return true;
            }
        }
        return false;
    }

    bool readWriteTreeRPC(
        Protocol::Client::ReadWriteTree_Request& request,
        Protocol::Client::ReadWriteTree_Response& response) {
        if (request.has_write()) {
            if (request.write().path() == "/foo" &&
                request.write().contents() == "hello") {
                request.mutable_write()->set_contents("world");
                return false;
            } else if (request.write().path() == "/except") {
                throw Client::TypeException("exception from callback");
            }
        }
        return false;
    }

    // heap-allocated for deferred construction
    std::unique_ptr<Client::Tree> tree;
};

TEST_F(ClientMockClientImplTest, callbacks) {
    auto callbacks = std::make_shared<MyCallbacks>();
    cluster.reset(new Client::Cluster(callbacks));
    Client::Tree tree = cluster->getTree();
    callbacks->tree.reset(new Client::Tree(tree));
    tree.writeEx("/foo", "hello");
    EXPECT_THROW(tree.readEx("/foo"),
                 Client::TimeoutException);
    EXPECT_EQ("world", tree.readEx("/bar"));
    EXPECT_THROW(tree.writeEx("/except", "here"),
                 Client::TypeException);
    // check that the object is still usable after an exception
    EXPECT_EQ("world", tree.readEx("/bar"));
    callbacks->tree.reset(); // break circular refcount
}

} // namespace LogCabin::<anonymous>
} // namespace LogCabin
