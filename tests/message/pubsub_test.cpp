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

#include "backend/common/logger.h"
#include "backend/message/nanomsg.h"

#include "gtest/gtest.h"
#include "harness.h"

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
  int result = 0;
  int sock = nn_socket (AF_SP, NN_PUB);
  if(sock < 0) {
    return sock;
  }

  result = nn_bind (sock, url);
  if(result < 0) {
    return result;
  }

  while (1) {
    char *d = date();
    int sz_d = strlen(d) + 1; // '\0' too
    LOG_INFO("SERVER: PUBLISHING DATE %s\n", d);
    int bytes = nn_send (sock, d, sz_d, 0);
    if(bytes != sz_d) {
      return -1;
    }

    sleep(1);
  }

  result = nn_shutdown (sock, 0);
  return result;
}

int client (const char *url, __attribute__((unused)) const char *name) {
  int result = 0;
  int sock = nn_socket (AF_SP, NN_SUB);
  if(sock < 0) {
    return sock;
  }

  // TODO learn more about publishing/subscribe keys
  result = nn_setsockopt (sock, NN_SUB, NN_SUB_SUBSCRIBE, "", 0);
  if(result < 0) {
    return result;
  }

  result = nn_connect (sock, url);
  if(result < 0) {
    return result;
  }

  while (1) {
    char *buf = NULL;
    int bytes = nn_recv (sock, &buf, NN_MSG, 0);
    if(bytes < 0) {
      return -1;
    }

    LOG_INFO("CLIENT (%s): RECEIVED %s\n", name, buf);
    nn_freemsg (buf);
  }

  result = nn_shutdown (sock, 0);
  return result;
}

TEST (PubsubTest, BasicTest) {

  std::thread send(server,"ipc:///tmp/pair.ipc");
  std::thread recv(client,"ipc:///tmp/pair.ipc","client0");

  send.detach();
  recv.detach();

  sleep(3);
}

}  // End test namespace
}  // End peloton namespace
