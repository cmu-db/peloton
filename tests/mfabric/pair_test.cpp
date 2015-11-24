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

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/mfabric/libnanomsg/src/nn.h"
#include "backend/mfabric/libnanomsg/src/pair.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Pair Tests
//===--------------------------------------------------------------------===//

#define NODE0 "node0"
#define NODE1 "node1"

int send_name(int sock, const char *name)
{
  printf("Send name \n");

  printf ("%s: SENDING \"%s\"\n", name, name);
  int sz_n = strlen (name) + 1; // '\0' too
  return nn_send (sock, name, sz_n, 0);
}

int recv_name(int sock, const char *name)
{
  printf("Recv name \n");

  char *buf = NULL;
  int result = nn_recv (sock, &buf, NN_MSG, 0);
  if (result > 0)
  {
    printf ("%s: RECEIVED \"%s\"\n", name, buf);
    nn_freemsg (buf);
  }
  return result;
}

int send_recv(int sock, const char *name)
{
  int to = 100;
  assert (nn_setsockopt (sock, NN_SOL_SOCKET, NN_RCVTIMEO, &to, sizeof (to)) >= 0);
  while(1)
  {
    printf("Send Recv \n");
    recv_name(sock, name);
    //sleep(1);
    send_name(sock, name);
  }
}

int node0 (const char *url)
{
  int sock = nn_socket (AF_SP, NN_PAIR);
  assert (sock >= 0);
  assert (nn_bind (sock, url) >= 0);
  send_recv(sock, NODE0);
  return nn_shutdown (sock, 0);
}

int node1 (const char *url)
{
  int sock = nn_socket (AF_SP, NN_PAIR);
  assert (sock >= 0);
  assert (nn_connect (sock, url) >= 0);
  send_recv(sock, NODE1);
  return nn_shutdown (sock, 0);
}

TEST(PairTest, BasicTest) {

  std::thread send(node0,"ipc:///tmp/pair.ipc");
  send.detach();
  std::thread recv(node1,"ipc:///tmp/pair.ipc");
  recv.detach();

  sleep(3);

}

}  // End test namespace
}  // End peloton namespace

