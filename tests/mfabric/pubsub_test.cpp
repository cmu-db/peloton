//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pubsub_test.cpp
//
// Identification: tests/mfabric/pubsub_test.cpp
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

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/mfabric/libnanomsg/src/nn.h"
#include "backend/mfabric/libnanomsg/src/pubsub.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Pub Sub Tests
//===--------------------------------------------------------------------===//


#define SERVER "server"
#define CLIENT "client"

char *date () {
  time_t raw = time (&raw);
  struct tm *info = localtime (&raw);
  char *text = asctime (info);
  text[strlen(text)-1] = '\0'; // remove '\n'
  return text;
}

int server (const char *url) {
  int sock = nn_socket (AF_SP, NN_PUB);
  assert (sock >= 0);
  assert (nn_bind (sock, url) >= 0);
  while (1)
  {
    char *d = date();
    int sz_d = strlen(d) + 1; // '\0' too
    printf ("SERVER: PUBLISHING DATE %s\n", d);
    int bytes = nn_send (sock, d, sz_d, 0);
    assert (bytes == sz_d);
    sleep(1);
  }
  return nn_shutdown (sock, 0);
}

int client (const char *url, const char *name) {
  int sock = nn_socket (AF_SP, NN_SUB);
  assert (sock >= 0);
  // TODO learn more about publishing/subscribe keys
  assert (nn_setsockopt (sock, NN_SUB, NN_SUB_SUBSCRIBE, "", 0) >= 0);
  assert (nn_connect (sock, url) >= 0);
  while (1)
  {
    char *buf = NULL;
    int bytes = nn_recv (sock, &buf, NN_MSG, 0);
    assert (bytes >= 0);
    printf ("CLIENT (%s): RECEIVED %s\n", name, buf);
    nn_freemsg (buf);
  }
  return nn_shutdown (sock, 0);
}

TEST (PubsubTest, BasicTest) {

  std::thread send(server,"ipc:///tmp/pair.ipc");
  send.detach();
  std::thread recv(client,"ipc:///tmp/pair.ipc","client0");
  recv.detach();

  sleep(3);
}

}  // End test namespace
}  // End peloton namespace
