//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pair_test.cpp
//
// Identification: tests/mfabric/pair_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string.h>
#include <unistd.h>
#include <cstdio>
#include <cassert>
#include <iostream>

#include "backend/common/logger.h"
#include "backend/message/nanomsg.h"

#include "gtest/gtest.h"
#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Pair Tests
//===--------------------------------------------------------------------===//

#define NODE0 "node0"
#define NODE1 "node1"

int send_name(int sock, const char *name){
  LOG_INFO("Send name");
  LOG_INFO("%s: SENDING \"%s\"", name, name);

  int sz_n = strlen (name) + 1; // '\0' too
  int result = nn_send (sock, name, sz_n, 0);

  return result;
}

int recv_name(int sock, __attribute__((unused)) const char *name){
  LOG_INFO("Recv name");

  char *buf = NULL;
  int result = nn_recv (sock, &buf, NN_MSG, 0);
  if (result > 0){
    LOG_INFO("%s: RECEIVED \"%s\"", name, buf);
    nn_freemsg (buf);
  }
  return result;
}

int send_recv(int sock, const char *name){
  int to = 100;

  int result = nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVTIMEO, &to, sizeof (to));
  if(result < 0) {
    return result;
  }

  while(1){
    LOG_INFO("Send Recv");
    recv_name(sock, name);

    //sleep(1);
    send_name(sock, name);
  }
}

int node0 (const char *url){
  int result = 0;
  int sock = nn_socket (AF_SP, NN_PAIR);
  if(sock < 0) {
    return sock;
  }

  result = nn_bind (sock, url);
  if(result < 0) {
    return result;
  }

  result = send_recv(sock, NODE0);
  if(result < 0) {
    return result;
  }

  result = nn_shutdown (sock, 0);
  return result;
}

int node1 (const char *url){
  int result = 0;
  int sock = nn_socket (AF_SP, NN_PAIR);
  if(sock < 0) {
    return sock;
  }

  result = nn_connect (sock, url);
  if(result < 0) {
    return result;
  }

  result = send_recv(sock, NODE1);
  if(result < 0) {
    return result;
  }

  result = nn_shutdown (sock, 0);
  return result;
}

TEST(PairTest, BasicTest) {

  std::thread send(node0,"ipc:///tmp/pair.ipc");
  std::thread recv(node1,"ipc:///tmp/pair.ipc");

  send.detach();
  recv.detach();

  sleep(3);
}

}  // End test namespace
}  // End peloton namespace

