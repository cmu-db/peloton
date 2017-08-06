//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_address_test.cpp
//
// Identification: test/distributed/network_address_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "common/exception.h"
#include "network/service/network_address.h"
#include "util/string_util.h"

namespace peloton {
namespace test {

class NetworkAddressTests : public PelotonTest {};

TEST_F(NetworkAddressTests, ParseTest) {
  // Valid address
  std::string ip = "127.0.0.1";
  int port = 9999;
  std::string address = StringUtil::Format("%s:%d", ip.c_str(), port);
  auto handle0 = network::service::NetworkAddress(address);
  EXPECT_EQ(ip, handle0.IpToString());
  EXPECT_EQ(port, handle0.GetPort());

  // Invalid address
  ip = "The terrier has diarrhea";
  address = StringUtil::Format("%s:%d", ip.c_str(), port);
  ASSERT_THROW(new network::service::NetworkAddress(address), peloton::Exception);
}
}
}
