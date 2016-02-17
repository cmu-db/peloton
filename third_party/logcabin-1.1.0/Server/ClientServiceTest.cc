/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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
#include <thread>

#include "build/Protocol/Client.pb.h"
#include "include/LogCabin/Debug.h"
#include "Core/Buffer.h"
#include "Core/ProtoBuf.h"
#include "Protocol/Common.h"
#include "RPC/ClientRPC.h"
#include "RPC/ClientSession.h"
#include "Server/Globals.h"
#include "Storage/FilesystemUtil.h"

namespace LogCabin {
namespace Server {
namespace {

using Protocol::Client::OpCode;
typedef RPC::ClientRPC::Status Status;
typedef RPC::ClientRPC::TimePoint TimePoint;

class ServerClientServiceTest : public ::testing::Test {
    ServerClientServiceTest()
        : storagePath(Storage::FilesystemUtil::mkdtemp())
        , globals()
        , session()
        , thread()
    {
    }

    // Test setup is deferred for handleRPCBadOpcode, which needs to bind the
    // server port in a new thread.
    void init() {
        if (!globals) {
            globals.reset(new Globals());
            globals->config.set("storageModule", "Memory");
            globals->config.set("uuid", "my-fake-uuid-123");
            globals->config.set("listenAddresses", "127.0.0.1");
            globals->config.set("serverId", "1");
            globals->config.set("storagePath", storagePath);
            globals->init();
            RPC::Address address("127.0.0.1", Protocol::Common::DEFAULT_PORT);
            address.refresh(RPC::Address::TimePoint::max());
            session = RPC::ClientSession::makeSession(
                globals->eventLoop,
                address,
                1024 * 1024,
                TimePoint::max(),
                Core::Config());
            thread = std::thread(&Globals::run, globals.get());
        }
    }

    ~ServerClientServiceTest()
    {
        if (globals) {
            globals->eventLoop.exit();
            thread.join();
        }
        Storage::FilesystemUtil::remove(storagePath);
    }

    void
    call(OpCode opCode,
         const google::protobuf::Message& request,
         google::protobuf::Message& response)
    {
        RPC::ClientRPC rpc(session,
                           Protocol::Common::ServiceId::CLIENT_SERVICE,
                           1, opCode, request);
        EXPECT_EQ(Status::OK, rpc.waitForReply(&response, NULL,
                                               TimePoint::max()))
            << rpc.getErrorMessage();
    }

    // Because of the EXPECT_DEATH call below, we need to take extra care to
    // clean up the storagePath tmpdir: destructors in the child process won't
    // get a chance.
    std::string storagePath;
    std::unique_ptr<Globals> globals;
    std::shared_ptr<RPC::ClientSession> session;
    std::thread thread;
};

TEST_F(ServerClientServiceTest, handleRPCBadOpcode) {
    init();
    Protocol::Client::GetServerInfo::Request request;
    Protocol::Client::GetServerInfo::Response response;
    int bad = 255;
    OpCode unassigned = static_cast<OpCode>(bad);
    LogCabin::Core::Debug::setLogPolicy({ // expect warning
        {"Server/ClientService.cc", "ERROR"},
        {"", "WARNING"},
    });
    RPC::ClientRPC rpc(session,
                       Protocol::Common::ServiceId::CLIENT_SERVICE,
                       1, unassigned, request);
    EXPECT_EQ(Status::INVALID_REQUEST, rpc.waitForReply(&response, NULL,
                                                        TimePoint::max()))
        << rpc.getErrorMessage();
}

////////// Tests for individual RPCs //////////

TEST_F(ServerClientServiceTest, verifyRecipient) {
    init();
    globals->clusterUUID.clear();
    Protocol::Client::VerifyRecipient::Request request;
    Protocol::Client::VerifyRecipient::Response response;

    call(OpCode::VERIFY_RECIPIENT, request, response);
    EXPECT_EQ("server_id: 1 "
              "ok: true ",
              response);

    request.set_cluster_uuid("myfirstcluster");
    request.set_server_id(1);
    call(OpCode::VERIFY_RECIPIENT, request, response);
    EXPECT_EQ("cluster_uuid: 'myfirstcluster' "
              "server_id: 1 "
              "ok: true ",
              response);

    request.set_cluster_uuid("mysecondcluster");
    call(OpCode::VERIFY_RECIPIENT, request, response);
    EXPECT_TRUE(Core::StringUtil::startsWith(response.error(),
                                             "Mismatched cluster UUIDs"));
    response.clear_error();
    EXPECT_EQ("cluster_uuid: 'myfirstcluster' "
              "server_id: 1 "
              "ok: false ",
              response);

    request.set_cluster_uuid("myfirstcluster");
    request.set_server_id(2);
    call(OpCode::VERIFY_RECIPIENT, request, response);
    EXPECT_TRUE(Core::StringUtil::startsWith(response.error(),
                                             "Mismatched server IDs"));
    response.clear_error();
    EXPECT_EQ("cluster_uuid: 'myfirstcluster' "
              "server_id: 1 "
              "ok: false ",
              response);
}

} // namespace LogCabin::Server::<anonymous>
} // namespace LogCabin::Server
} // namespace LogCabin
