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
#include "networking/rpc_server.h"
#include "networking/peloton_service.h"

namespace peloton {
namespace test {

class RpcServerTests : public PelotonTest {};

TEST_F(RpcServerTests, BasicTest) {
  // FIXME: If we comment out the entire test case, then there'll be some static
  // variable related to protobuf that won't be freed at the TearDown stage of
  // PelotonTest. However this only begins with this merge:
  // 8927d99b2fedd265e2ba331966a0d2abbfa2ff50
  /*
    distributed::RpcServer rpc_server(9001);
    distributed::PelotonService service;

    bool status = rpc_server.RegisterService(&service);
    EXPECT_EQ(status, true);

    auto ptr = rpc_server.FindMethod(1);
    EXPECT_EQ(ptr, nullptr);
  */
}
}
}
