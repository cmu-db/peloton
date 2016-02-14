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

using Core::ProtoBuf::fromString;
using Core::StringUtil::format;


#if DEBUG
class ClientClusterTest : public ::testing::Test {
  public:
    typedef Client::LeaderRPCMock::OpCode OpCode;
    ClientClusterTest()
        : cluster(new Client::Cluster("-MOCK-SKIP-INIT-"))
        , mockRPC()
    {
        Client::ClientImpl* client =
            dynamic_cast<Client::ClientImpl*>(cluster->clientImpl.get());
        mockRPC = new Client::LeaderRPCMock();
        client->leaderRPC = std::unique_ptr<Client::LeaderRPCBase>(mockRPC);
        cluster->clientImpl->init("127.0.0.1:0");

        client->exactlyOnceRPCHelper.client = NULL;
    }
    std::unique_ptr<Client::Cluster> cluster;
    Client::LeaderRPCMock* mockRPC;
    ClientClusterTest(const ClientClusterTest&) = delete;
    ClientClusterTest& operator=(const ClientClusterTest&) = delete;
};


// Client::Cluster(FOR_TESTING) tested in MockClientImplTest.cc

TEST_F(ClientClusterTest, constructor) {
    // TODO(ongaro): test
}

#endif /* DEBUG */

// TODO(ongaro): test getConfiguration, setConfiguraton

// This is to test the serialization/deserialization of Tree RPCs in both the
// client library and Tree/ProtoBuf.h.
class ClientTreeTest : public ::testing::Test {
  public:
    ClientTreeTest()
        : cluster(std::make_shared<Client::TestingCallbacks>())
        , tree(cluster.getTree())
    {
    }
    Client::Cluster cluster;
    Client::Tree tree;
    ClientTreeTest(const ClientTreeTest&) = delete;
    ClientTreeTest& operator=(const ClientTreeTest&) = delete;
};

using Client::Result;
using Client::Status;

#define EXPECT_OK(c) do { \
    Result result = (c); \
    EXPECT_EQ(Status::OK, result.status) << result.error; \
} while (0)

TEST_F(ClientTreeTest, assignment)
{
    Client::Tree tree2 =
        Client::Cluster(std::make_shared<Client::TestingCallbacks>())
        .getTree();
    tree2.setWorkingDirectory("/foo/bar");
    tree2 = tree;
    EXPECT_EQ("/", tree2.getWorkingDirectory());
}

TEST_F(ClientTreeTest, setWorkingDirectory)
{
    EXPECT_OK(tree.setWorkingDirectory("foo"));
    EXPECT_EQ("/foo", tree.getWorkingDirectory());
    Result result = tree.setWorkingDirectory("../..");
    EXPECT_EQ(Status::INVALID_ARGUMENT, result.status);
    EXPECT_EQ("Path '../..' from working directory '/foo' attempts to "
              "look up directory above root ('/')", result.error);
    EXPECT_EQ("invalid from prior call to setWorkingDirectory('../..') "
              "relative to '/foo'",
              tree.getWorkingDirectory());
    result = tree.makeDirectory("x");
    EXPECT_EQ(Status::INVALID_ARGUMENT, result.status);
    EXPECT_EQ("Can't use relative path 'x' from working directory 'invalid "
              "from prior call to setWorkingDirectory('../..') relative to "
              "'/foo'' (working directory should be an absolute path)",
              result.error);
    EXPECT_OK(tree.setWorkingDirectory("/"));
    EXPECT_EQ("/", tree.getWorkingDirectory());
}

TEST_F(ClientTreeTest, getCondition)
{
    EXPECT_EQ((Client::Condition {"", ""}),
              tree.getCondition());
    tree.setCondition("a", "b");
    EXPECT_EQ((Client::Condition {"/a", "b"}),
              tree.getCondition());
    tree.setCondition("", "asdf");
    EXPECT_EQ((Client::Condition {"", ""}),
              tree.getCondition());
    tree.setCondition("", "");
    EXPECT_EQ((Client::Condition {"", ""}),
              tree.getCondition());
}

TEST_F(ClientTreeTest, setCondition)
{
    EXPECT_OK(tree.setCondition("", ""));

    Result result = tree.setCondition("/..", "x");
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              result.status);
    EXPECT_EQ("Path '/..' from working directory '/' attempts to look up "
              "directory above root ('/')",
              result.error);
    EXPECT_EQ((Client::Condition {
                   "invalid from prior call to setCondition('/..') "
                   "relative to '/'",
                   "x"
               }),
               tree.getCondition());
}

TEST_F(ClientTreeTest, getTimeout)
{
    EXPECT_EQ(0UL, tree.getTimeout());
    tree.setTimeout(43UL);
    EXPECT_EQ(43UL, tree.getTimeout());
}

TEST_F(ClientTreeTest, setTimeout)
{
    tree.setTimeout(43UL);
    EXPECT_EQ(43UL, tree.getTimeout());
    tree.setTimeout(0UL);
    EXPECT_EQ(0UL, tree.getTimeout());
}

TEST_F(ClientTreeTest, makeDirectory)
{
    EXPECT_OK(tree.makeDirectory("/foo"));
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.makeDirectory("/..").status);
    EXPECT_FALSE(tree.makeDirectory("/..").error.empty());
    std::vector<std::string> children;
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{"foo/"}),
              children);
}

TEST_F(ClientTreeTest, listDirectory)
{
    std::vector<std::string> children;
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.listDirectory("/..", children).status);
    EXPECT_FALSE(tree.listDirectory("/..", children).error.empty());
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{}),
              children);
    EXPECT_OK(tree.makeDirectory("/foo"));
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{"foo/"}),
              children);
}

TEST_F(ClientTreeTest, removeDirectory)
{
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.removeDirectory("/..").status);
    EXPECT_OK(tree.makeDirectory("/foo"));
    EXPECT_OK(tree.removeDirectory("/foo"));
    std::vector<std::string> children;
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{}),
              children);
}

TEST_F(ClientTreeTest, write)
{
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.write("/..", "bar").status);
    EXPECT_OK(tree.write("/foo", "bar"));
    std::string contents;
    EXPECT_OK(tree.read("/foo", contents));
    EXPECT_EQ("bar", contents);
}

TEST_F(ClientTreeTest, read)
{
    std::string contents;
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.read("/..", contents).status);
    EXPECT_OK(tree.write("/foo", "bar"));
    EXPECT_OK(tree.read("/foo", contents));
    EXPECT_EQ("bar", contents);
}

TEST_F(ClientTreeTest, removeFile)
{
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.removeFile("/..").status);
    EXPECT_OK(tree.write("/foo", "bar"));
    EXPECT_OK(tree.removeFile("/foo"));
    std::vector<std::string> children;
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{}),
              children);
}

TEST_F(ClientTreeTest, conditions)
{
    tree.setCondition("/a", "c");
    EXPECT_EQ((std::pair<std::string, std::string> {"/a", "c"}),
              tree.getCondition());
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.makeDirectory("/foo").status);
    std::vector<std::string> children;
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.listDirectory("/", children).status);
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.removeDirectory("/").status);
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.write("/a", "c").status);
    std::string contents;
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.read("/a", contents).status);
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.removeFile("/a").status);

    tree.setCondition("", "");
    tree.writeEx("/a", "c");
    tree.setCondition("/a", "c");
    EXPECT_EQ(Status::OK,
              tree.makeDirectory("/foo").status);
    EXPECT_EQ(Status::OK,
              tree.listDirectory("/foo", children).status);
    EXPECT_EQ(Status::OK,
              tree.removeDirectory("/foo").status);
    EXPECT_EQ(Status::OK,
              tree.write("/b", "c").status);
    EXPECT_EQ(Status::OK,
              tree.read("/b", contents).status);
    EXPECT_EQ(Status::OK,
              tree.removeFile("/b").status);
}

TEST_F(ClientTreeTest, conditions_withWorkingDirectory)
{
    tree.setWorkingDirectory("/baz");
    tree.writeEx("bar", "d");
    tree.setCondition("bar", "c");
    EXPECT_EQ((std::pair<std::string, std::string> {"/baz/bar", "c"}),
              tree.getCondition());
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.makeDirectory("foo").status);
    std::vector<std::string> children;
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.listDirectory("", children).status);
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.removeDirectory("").status);
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.write("a", "c").status);
    std::string contents;
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.read("a", contents).status);
    EXPECT_EQ(Status::CONDITION_NOT_MET,
              tree.removeFile("a").status);

    tree.setCondition("bar", "d");
    EXPECT_EQ((std::pair<std::string, std::string> {"/baz/bar", "d"}),
              tree.getCondition());
    EXPECT_EQ(Status::OK,
              tree.makeDirectory("foo").status);
    EXPECT_EQ(Status::OK,
              tree.listDirectory("foo", children).status);
    EXPECT_EQ(Status::OK,
              tree.removeDirectory("foo").status);
    EXPECT_EQ(Status::OK,
              tree.write("a", "c").status);
    EXPECT_EQ(Status::OK,
              tree.read("a", contents).status);
    EXPECT_EQ(Status::OK,
              tree.removeFile("a").status);
}

} // namespace LogCabin::<anonymous>
} // namespace LogCabin
