//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_client_test.cpp
//
// Identification: /peloton/tests/networking/rpc_client_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/networking/rpc_client.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/tcp_connection.h"
#include "backend/networking/peloton_service.h"
#include "backend/networking/abstract_service.pb.h"

#include <functional>

namespace peloton {
namespace test {

class RpcClientTests : public PelotonTest {};

void Coordinator() {

    google::protobuf::Service* service = NULL;

    try {
        networking::RpcServer rpc_server(PELOTON_SERVER_PORT);
        service = new peloton::networking::PelotonService();
        rpc_server.RegisterService(service);
        rpc_server.Start();
    } catch (std::exception& e) {
        std::cerr << "STD EXCEPTION : " << e.what() << std::endl;
        delete service;
    } catch (...) {
        std::cerr << "UNTRAPPED EXCEPTION " << std::endl;
        delete service;
    }
}

TEST_F(RpcClientTests, BasicTest) {

  // Lanch coordinator to recv msg
//  std::thread coordinator(Coordinator);
//  coordinator.detach();

  networking::PelotonService service;

  // Heartbeat is 13 for index
  const google::protobuf::MethodDescriptor* method_des = service.descriptor()->method(13);

  std::string methodname = std::string( method_des->full_name() );

  // crate a connection to prcess the rpc send and recv
  std::shared_ptr<networking::Connection> conn(new networking::Connection(-1, NULL));

  // Client connection must set method name
  conn->SetMethodName(methodname);

  const std::string name = conn->GetMethodName();

  bool b_name = (methodname == name);

  EXPECT_EQ(b_name, true);

  ///////////Generate data//////////////
  std::hash<std::string> string_hash_fn;

  networking::HeartbeatRequest request;

  request.set_sender_site(12);
  request.set_last_transaction_id(34);

  // we use unit64_t because we should specify the exact length
  uint64_t opcode = string_hash_fn(methodname);

  // prepare the sending buf
  uint32_t msg_len = request.ByteSize() + sizeof(opcode);

  // total length of the message: header length (4bytes) + message length (8bytes + ...)
  assert(HEADERLEN == sizeof(msg_len));
  char buf[sizeof(msg_len) + msg_len];

  // copy the header into the buf
  memcpy(buf, &msg_len, sizeof(msg_len));

  // copy the hashcode into the buf, following the header
  assert(OPCODELEN == sizeof(opcode));
  memcpy(buf + sizeof(msg_len), &opcode, sizeof(opcode));

  // call protobuf to serialize the request message into sending buf
  request.SerializeToArray(buf + sizeof(msg_len) + sizeof(opcode), request.ByteSize());

  //  ///////////Generate address///////////////////

    networking::NetworkAddress addr_(PELOTON_ENDPOINT_ADDR);

  ////////////Send data////////////////

  // write data into sending buffer, when using libevent we don't need loop send
  bool b_write = conn->AddToWriteBuffer(buf, sizeof(buf));

  EXPECT_EQ(b_write, true);

  // Connect to server with given address
  bool bconn = conn->Connect(addr_);

  EXPECT_EQ(bconn, true);
}

}
}

