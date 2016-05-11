//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_server_test.cpp
//
// Identification: tests/networking/rpc_server_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_service.h"

namespace peloton {
namespace test {

class RpcServerTests : public PelotonTest {};

TEST_F(RpcServerTests, BasicTest) {
  networking::RpcServer rpc_server(9001);
  networking::PelotonService service;

  bool status = rpc_server.RegisterService(&service);
  EXPECT_EQ(status, true);

  auto ptr = rpc_server.FindMethod(1);
  EXPECT_EQ(ptr, NULL);
}
}
}
