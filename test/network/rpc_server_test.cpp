//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_server_test.cpp
//
// Identification: test/distributed/rpc_server_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "network/service/rpc_server.h"
#include "network/service/peloton_service.h"

namespace peloton {
namespace test {

class RpcServerTests : public PelotonTest {};

TEST_F(RpcServerTests, BasicTest) {
  // FIXME: If we comment out the entire test case, then there'll be some static
  // variable related to protobuf that won't be freed at the TearDown stage of
  // PelotonTest. However this only begins with this merge:
  // 8927d99b2fedd265e2ba331966a0d2abbfa2ff50
  /*
    network::service::RpcServer rpc_server(9001);
    network::service::PelotonService service;

    bool status = rpc_server.RegisterService(&service);
    EXPECT_TRUE(status);

    auto ptr = rpc_server.FindMethod(1);
    EXPECT_EQ(ptr, nullptr);
  */
}
}
}
