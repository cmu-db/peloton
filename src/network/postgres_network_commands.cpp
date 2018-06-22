//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_network_commands.cpp
//
// Identification: src/network/postgres_network_commands.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "parser/postgresparser.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/peloton_server.h"
#include "network/postgres_network_commands.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace peloton {
namespace network {

Transition StartupCommand::Exec(PostgresProtocolInterpreter &protocol_object,
                                WriteQueue &out,
                                size_t) {
  // Always flush startup response
  out.ForceFlush();
  int32_t proto_version = input_packet_.buf_->ReadInt(sizeof(int32_t));
  LOG_INFO("protocol version: %d", proto_version);
  if (proto_version == SSL_MESSAGE_VERNO) {
    // SSL Handshake initialization
    // TODO(Tianyu): This static method probably needs to be moved into
    // settings manager
    bool ssl_able = (PelotonServer::GetSSLLevel() != SSLLevel::SSL_DISABLE);
    out.WriteSingleBytePacket(ssl_able
                              ? NetworkMessageType::SSL_YES
                              : NetworkMessageType::SSL_NO);
    return ssl_able ? Transition::NEED_SSL_HANDSHAKE : Transition::PROCEED;
  } else {
    // Normal Initialization
    if (PROTO_MAJOR_VERSION(proto_version) != 3) {
      // Only protocol version 3 is supported
      LOG_ERROR("Protocol error: Only protocol version 3 is supported.");
      PostgresWireUtilities::SendErrorResponse(
          out, {{NetworkMessageType::HUMAN_READABLE_ERROR,
                 "Protocol Version Not Support"}});
      return Transition::TERMINATE;
    }

    std::string token, value;
    // TODO(Yuchen): check for more malformed cases
    // Read out startup package info
    while (input_packet_.buf_->HasMore()) {
      token = input_packet_.buf_->ReadString();
      LOG_TRACE("Option key is %s", token.c_str());
      // TODO(Tianyu): Why does this commented out line need to be here?
      // if (!input_packet_.buf_->HasMore()) break;
      value = input_packet_.buf_->ReadString();
      LOG_TRACE("Option value is %s", value.c_str());
      // TODO(Tianyu): We never seem to use this crap?
      protocol_object.AddCommandLineOption(token, value);
      // TODO(Tianyu): Do this after we are done refactoring traffic cop
//      if (token.compare("database") == 0) {
//        traffic_cop_->SetDefaultDatabaseName(value);
//      }
    }

    // Startup Response, for now we do not do any authentication
    PostgresWireUtilities::SendStartupResponse(out);
    protocol_object.FinishStartup();
    return Transition::PROCEED;
  }
}


} // namespace network
} // namespace peloton