//
// Created by Siddharth Santurkar on 31/3/16.
//

#ifndef WIRE_H
#define WIRE_H

#include "socket_base.h"
#include "sqlite.h"
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>
#include <boost/assign/list_of.hpp>
#include "assert.h"
#include "cache.h"
#include "cache_entry.h"

/* TXN state definitions */
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
  SocketManager<PktBuf>* sock;
  std::string dbname;
  std::string user;
  std::unordered_map<std::string, std::string> cmdline_options;

  inline Client(SocketManager<PktBuf>* sock) : sock(sock) {}
};


struct Packet {
  PktBuf buf;
  size_t len;
  size_t ptr;
  uchar msg_type;

  // reserve buf's size as maximum packet size
  inline Packet() { reset(); }

  inline void reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    len = ptr = msg_type = 0;
  }
};

class PacketManager {
  Client client;

  std::shared_ptr<CacheEntry> unnamed_entry;
  uchar txn_state;
  bool skipped_stmt_;
  std::string skipped_query_;
  std::string skipped_query_type_;

  wiredb::Sqlite db;

  static const std::unordered_map<std::string, std::string> parameter_status_map;

  void send_error_response(
      std::vector<std::pair<uchar, std::string>> error_status,
      ResponseBuffer& responses);

  void send_ready_for_query(uchar txn_status, ResponseBuffer& responses);

  void put_dummy_row_desc(ResponseBuffer& responses);

  void put_row_desc(std::vector<wiredb::FieldInfoType>& rowdesc, ResponseBuffer& responses);

  void send_data_rows(std::vector<wiredb::ResType>& results, int colcount, int &rows_affected,  ResponseBuffer& responses);

  void put_dummy_data_row(int colcount, int start, ResponseBuffer& responses);

  void complete_command(const std::string& query_type,
                        int rows, ResponseBuffer& responses);

  void send_empty_query_response(ResponseBuffer& responses);

  void make_hardcoded_parameter_status(ResponseBuffer& responses, const std::pair<std::string, std::string>& kv);

  bool hardcoded_execute_filter(std::string query_type);

  void exec_query_message(Packet *pkt, ResponseBuffer &responses);

  void exec_parse_message(Packet *pkt, ResponseBuffer &responses);

  void exec_bind_message(Packet *pkt, ResponseBuffer &responses);

  void exec_describe_message(Packet *pkt, ResponseBuffer &responses);

  void exec_execute_message(Packet *pkt, ResponseBuffer &response, ThreadGlobals& globals);

  void close_client();

 public:

  inline PacketManager(SocketManager<PktBuf>* sock) :
      client(sock), txn_state(TXN_IDLE) {}

  bool process_startup_packet(Packet* pkt, ResponseBuffer& responses);

  bool process_packet(Packet* pkt, ThreadGlobals &globals, ResponseBuffer& responses);

  void manage_packets(ThreadGlobals& globals);
};
}
}

#endif  // WIRE_H
