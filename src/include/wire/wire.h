//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire.h
//
// Identification: src/include/wire/wire.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "socket_base.h"
#include "sqlite.h"
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>
#include <boost/assign/list_of.hpp>

#include "common/cache.h"
#include "statement.h"

// TXN state definitions
#define BUFFER_INIT_SIZE 100
#define TXN_IDLE 'I'
#define TXN_BLOCK 'T'
#define TXN_FAIL 'E'

namespace peloton {
namespace wire {

typedef std::vector<uchar> PktBuf;

struct Packet;

typedef std::vector<std::unique_ptr<Packet>> ResponseBuffer;

struct Client {
  SocketManager<PktBuf>* sock;  // handle to socket manager

  // Authentication details
  std::string dbname;
  std::string user;
  std::unordered_map<std::string, std::string> cmdline_options;

  inline Client(SocketManager<PktBuf>* sock) : sock(sock) {}
};

struct Packet {
  PktBuf buf;  // stores packet contents
  size_t len;      // size of packet
  size_t ptr;      // PktBuf cursor
  uchar msg_type;  // header

  // reserve buf's size as maximum packet size
  inline Packet() { Reset(); }

  inline void Reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    len = ptr = msg_type = 0;
  }
};

class PacketManager {
  Client client;

  // Manage standalone queries
  std::shared_ptr<Statement> unnamed_entry;

  // gloabl txn state
  uchar txn_state;

  // state to mang skipped queries
  bool skipped_stmt_ = false;
  std::string skipped_query_;
  std::string skipped_query_type_;

  wire::Sqlite db;  // dbhandler to use sqlite

  static const std::unordered_map<std::string, std::string>
      parameter_status_map;

  /* Note: The responses argument in every subsequent function
   * is used to batch all the generated packets ofr that unit */

  // Generic error protocol packet
  void SendErrorResponse(
      std::vector<std::pair<uchar, std::string>> error_status,
      ResponseBuffer& responses);

  // Sends ready for query packet to the frontend
  void SendReadyForQuery(uchar txn_status, ResponseBuffer& responses);

  // Sends the attribute headers required by SELECT queries
  void PutRowDesc(std::vector<wire::FieldInfoType>& rowdesc,
                  ResponseBuffer& responses);

  // Send each row, one packet at a time, used by SELECT queries
  void SendDataRows(std::vector<wire::ResType>& results, int colcount,
                    int& rows_affected, ResponseBuffer& responses);

  // Used to send a packet that indicates the completion of a query. Also has
  // txn state mgmt
  void CompleteCommand(const std::string& query_type, int rows,
                       ResponseBuffer& responses);

  // Specific response for empty or NULL queries
  void SendEmptyQueryResponse(ResponseBuffer& responses);

  /* Helper function used to make hardcoded ParameterStatus('S')
   * packets during startup
   */
  void MakeHardcodedParameterStatus(
      ResponseBuffer& responses, const std::pair<std::string, std::string>& kv);

  /* SQLite doesn't support "SET" and "SHOW" SQL commands.
   * Also, duplicate BEGINs and COMMITs shouldn't be executed.
   * This function helps filtering out the execution for such cases
   */
  bool HardcodedExecuteFilter(std::string query_type);

  /* Execute a Simple query protocol message */
  void ExecQueryMessage(Packet* pkt, ResponseBuffer& responses);

  /* Process the PARSE message of the extended query protocol */
  void ExecParseMessage(Packet* pkt, ResponseBuffer& responses);

  /* Process the BIND message of the extended query protocol */
  void ExecBindMessage(Packet* pkt, ResponseBuffer& responses);

  /* Process the DESCRIBE message of the extended query protocol */
  void ExecDescribeMessage(Packet* pkt, ResponseBuffer& responses);

  /* Process the EXECUTE message of the extended query protocol */
  void ExecExecuteMessage(Packet* pkt, ResponseBuffer& response);

  /* closes the socket connection with the client */
  void CloseClient();

 public:
  inline PacketManager(SocketManager<PktBuf>* sock)
      : client(sock), txn_state(TXN_IDLE) {}

  /* Startup packet processing logic */
  bool ProcessStartupPacket(Packet* pkt, ResponseBuffer& responses);

  /* Main switch case wrapper to process every packet apart from the startup
   * packet */
  bool ProcessPacket(Packet* pkt, ResponseBuffer& responses);

  /* Protocol manager */
  void ManagePackets();
};

}  // End wire namespace
}  // End peloton namespace
