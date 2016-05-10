//
// Created by Siddharth Santurkar on 15/4/16.
//

#include "harness.h"

#include <vector>
#include "backend/wire/wire.h"
#include "backend/wire/globals.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Wire Tests
//===--------------------------------------------------------------------===//

class WireTests : public PelotonTest {};

wire::PacketManager* BuildPacketManager() {
  // no sockets
  return new wire::PacketManager(nullptr);
}

TEST_F(WireTests, StartupTest) {
  std::unique_ptr<wire::PacketManager> pktmgr(BuildPacketManager());
  std::vector<std::unique_ptr<wire::Packet>> responses;

  // valid packet
  wire::Packet startup_pkt;
  startup_pkt.len = 80;
  startup_pkt.buf = {0,   3,   0,   0,   117, 115, 101, 114, 0,   112, 111, 115,
                     116, 103, 114, 101, 115, 0,   100, 97,  116, 97,  98,  97,
                     115, 101, 0,   112, 111, 115, 116, 103, 114, 101, 115, 0,
                     97,  112, 112, 108, 105, 99,  97,  116, 105, 111, 110, 95,
                     110, 97,  109, 101, 0,   112, 115, 113, 108, 0,   99,  108,
                     105, 101, 110, 116, 95,  101, 110, 99,  111, 100, 105, 110,
                     103, 0,   85,  84,  70,  56,  0,   0};

  bool status = pktmgr->process_startup_packet(&startup_pkt, responses);

  EXPECT_EQ(true, status);

  EXPECT_EQ(13, responses.size());

  // auth-ok packet
  EXPECT_EQ('R', responses[0]->msg_type);
  EXPECT_EQ(4, responses[0]->len);

  // parameter-status pkt
  EXPECT_EQ('S', responses[1]->msg_type);
  // EXPECT_EQ(1, responses[1]->len);

  // ready-for-query packet
  EXPECT_EQ('Z', responses[12]->msg_type);
  EXPECT_EQ(1, responses[12]->len);

}

//TEST_F(WireTests, StartupErrorTest) {
//  std::unique_ptr<wire::PacketManager> pktmgr(BuildPacketManager());
//  std::vector<std::unique_ptr<wire::Packet>> responses;
//
//  // valid packet
//  wire::Packet startup_pkt;
//  startup_pkt.len = 79;
//  startup_pkt.buf = {0,   3,   0,   0,   117, 115, 101, 0,   112, 111, 115, 116,
//                     103, 114, 101, 115, 0,   100, 97,  116, 97,  98,  97,  115,
//                     101, 0,   112, 111, 115, 116, 103, 114, 101, 115, 0,   97,
//                     112, 112, 108, 105, 99,  97,  116, 105, 111, 110, 95,  110,
//                     97,  109, 101, 0,   112, 115, 113, 108, 0,   99,  108, 105,
//                     101, 110, 116, 95,  101, 110, 99,  111, 100, 105, 110, 103,
//                     0,   85,  84,  70,  56,  0,   0};
//
//  bool status = pktmgr->process_startup_packet(&startup_pkt, responses);
//
//  EXPECT_EQ(false, status);
//
//  EXPECT_EQ(2, responses.size());
//
//  // auth-ok packet
//  EXPECT_EQ('R', responses[0]->msg_type);
//  EXPECT_EQ(4, responses[0]->len);
//
//  // ready-for-query packet
//  EXPECT_EQ('E', responses[1]->msg_type);
//}

//TEST_F(WireTests, SimpleQueryTest) {
//  std::unique_ptr<wire::PacketManager> pktmgr(BuildPacketManager());
//  std::vector<std::unique_ptr<wire::Packet>> responses;
//  wire::ThreadGlobals globals;
//
//  // valid packet
//  wire::Packet startup_pkt;
//  startup_pkt.msg_type = 'Q';
//  startup_pkt.len = 3;
//  startup_pkt.buf = {97, 59, 0};
//
//  bool status = pktmgr->process_packet(&startup_pkt, globals, responses);
//
//  EXPECT_EQ(true, status);
//  EXPECT_EQ(8, responses.size());
//
//  // row desc
//  EXPECT_EQ('T', responses[0]->msg_type);
//
//  // command complete packet
//  auto ptr = responses.end() - 2;
//  EXPECT_EQ('C', ptr->get()->msg_type);
//
//  // ready for query packet
//  ptr = responses.end() - 1;
//  EXPECT_EQ('Z', ptr->get()->msg_type);
//}

TEST_F(WireTests, EmptyQueryTest) {
  std::unique_ptr<wire::PacketManager> pktmgr(BuildPacketManager());
  std::vector<std::unique_ptr<wire::Packet>> responses;
  wire::ThreadGlobals globals;

  // valid packet
  wire::Packet startup_pkt;
  startup_pkt.msg_type = 'Q';
  startup_pkt.len = 2;
  startup_pkt.buf = {59, 0};

  bool status = pktmgr->process_packet(&startup_pkt, globals, responses);

  EXPECT_EQ(true, status);
  EXPECT_EQ(2, responses.size());

  // empty query packet
  EXPECT_EQ('I', responses[0]->msg_type);

  // ready for query packet
  EXPECT_EQ('Z', responses[1]->msg_type);
}

//TEST_F(WireTests, MultiQueryTest) {
//  std::unique_ptr<wire::PacketManager> pktmgr(BuildPacketManager());
//  std::vector<std::unique_ptr<wire::Packet>> responses;
//  wire::ThreadGlobals globals;
//
//  // valid packet
//  wire::Packet startup_pkt;
//  startup_pkt.msg_type = 'Q';
//  startup_pkt.len = 5;
//  startup_pkt.buf = {97, 59, 98, 59, 0};
//
//  bool status = pktmgr->process_packet(&startup_pkt, globals, responses);
//
//  EXPECT_EQ(true, status);
//  EXPECT_EQ(15, responses.size());
//
//  // row desc
//  EXPECT_EQ('T', responses[0]->msg_type);
//
//  // command complete packet
//  auto ptr = responses.end() - 2;
//  EXPECT_EQ('C', ptr->get()->msg_type);
//
//  // ready for query packet
//  ptr = responses.end() - 1;
//  EXPECT_EQ('Z', ptr->get()->msg_type);
//}

TEST_F(WireTests, QuitTest) {
  std::unique_ptr<wire::PacketManager> pktmgr(BuildPacketManager());
  std::vector<std::unique_ptr<wire::Packet>> responses;
  wire::ThreadGlobals globals;

  // valid packet
  wire::Packet startup_pkt;
  startup_pkt.msg_type = 'X';

  bool status = pktmgr->process_packet(&startup_pkt, globals, responses);

  // false to close server's socket
  EXPECT_EQ(false, status);
}
}
}
