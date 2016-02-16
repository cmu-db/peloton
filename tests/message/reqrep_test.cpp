//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// reqrep_test.cpp
//
// Identification: tests/mfabric/reqrep_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <string.h>
#include <unistd.h>
#include <string.h>
#include <cstdio>
#include <cassert>
#include <ctime>
#include <thread>

#include "backend/common/logger.h"
#include "backend/message/nanomsg.h"

#include "gtest/gtest.h"
#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Request Reply Tests
//===--------------------------------------------------------------------===//

#define NODE0 "node0"
#define NODE1 "node1"

int node0 (const char *url){
  int result = 0;
  int sock = nn_socket (AF_SP, NN_PULL);
  if(sock < 0) {
    return sock;
  }

  result = nn_bind (sock, url);
  if(result < 0) {
    return result;
  }

  while (1) {
    char *buf = NULL;
    int bytes = nn_recv (sock, &buf, NN_MSG, 0);
    if(bytes < 0) {
      return bytes;
    }

    LOG_INFO("NODE0: RECEIVED \"%s\"", buf);
    nn_freemsg (buf);
  }
}

int node1 (const char *url, const char *msg) {
  int result = 0;
  int sz_msg = strlen (msg) + 1; // '\0' too
  int sock = nn_socket (AF_SP, NN_PUSH);
  if(sock < 0) {
    return sock;
  }

  result = nn_connect (sock, url);
  if(result < 0) {
    return result;
  }

  LOG_INFO("NODE1: SENDING \"%s\"", msg);
  int bytes = nn_send (sock, msg, sz_msg, 0);
  if(bytes != sz_msg) {
    return bytes;
  }

  result = nn_shutdown (sock, 0);
  return result;
}

TEST (ReqRepTest, BasicTest) {

  std::thread send(node0,"ipc:///tmp/pair.ipc");
  std::thread recv(node1,"ipc:///tmp/pair.ipc","client1");

  send.detach();
  recv.detach();

  sleep(3);
}

}  // End test namespace
}  // End peloton namespace
