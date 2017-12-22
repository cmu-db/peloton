//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_client_test.cpp
//
// Identification: test/distributed/rpc_client_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "network/service/network_address.h"
#include "network/service/rpc_type.h"
#include "network/service/rpc_client.h"
#include "network/service/rpc_server.h"
#include "network/service/tcp_connection.h"
#include "network/service/peloton_service.h"
#include "peloton/proto/abstract_service.pb.h"

#include <functional>

namespace peloton {
namespace test {

class RpcClientTests : public PelotonTest {};

TEST_F(RpcClientTests, BasicTest) {
  // FIXME: If we comment out the entire test case, then there'll be some static
  // variable related to protobuf that won't be freed at the TearDown stage of
  // PelotonTest. However this only begins with this merge:
  // 8927d99b2fedd265e2ba331966a0d2abbfa2ff50
  /*
    network::service::RpcServer rpc_server(PELOTON_SERVER_PORT);
    network::service::PelotonService service;
    rpc_server.RegisterService(&service);

    // Heartbeat is 13 for index
    const google::protobuf::MethodDescriptor* method_des =
        service.descriptor()->method(13);

    std::string methodname = std::string(method_des->full_name());

    ///////////Generate data//////////////
    std::hash<std::string> string_hash_fn;

    // we use unit64_t because we should specify the exact length
    uint64_t opcode = string_hash_fn(methodname);

    network::service::HeartbeatRequest request;

    request.set_sender_site(12);
    request.set_last_transaction_id(34);

    // set the type
    uint16_t type = ::MSG_TYPE_REQ;

    // prepare the sending buf
    uint32_t msg_len = request.ByteSize() + sizeof(type) + sizeof(opcode);

    // total length of the message: header length (4bytes) + message length
    // (8bytes + ...)
    PL_ASSERT(HEADERLEN == sizeof(msg_len));
    char buf[sizeof(msg_len) + msg_len];

    // copy the header into the buf
    PL_MEMCPY(buf, &msg_len, sizeof(msg_len));

    // copy the type into the buf
    PL_MEMCPY(buf + sizeof(msg_len), &type, sizeof(type));

    // copy the hashcode into the buf, following the header
    PL_ASSERT(OPCODELEN == sizeof(opcode));
    PL_MEMCPY(buf + sizeof(msg_len) + sizeof(type), &opcode, sizeof(opcode));

    // call protobuf to serialize the request message into sending buf
    request.SerializeToArray(
        buf + sizeof(msg_len) + sizeof(type) + sizeof(opcode),
        request.ByteSize());

    std::string methodname2 =
    rpc_server.FindMethod(opcode)->method_->full_name();
    bool comp = (methodname == methodname2);
    EXPECT_TRUE(comp);
  */
}
}
}
