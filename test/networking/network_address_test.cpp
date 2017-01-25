//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_address_test.cpp
//
// Identification: test/networking/network_address_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "networking/network_address.h"
#include "util/string_util.h"

namespace peloton {
namespace test {

class NetworkAddressTests : public PelotonTest {};

TEST_F(NetworkAddressTests, ParseTest) {
  std::string ip = "127.0.0.1";
  int port = 9999;
  std::string address = StringUtil::Format("%s:%d", ip.c_str(), port);

  auto handle = networking::NetworkAddress(address);
  EXPECT_EQ(ip, handle.IpToString());
  EXPECT_EQ(port, handle.GetPort());
}
}
}
