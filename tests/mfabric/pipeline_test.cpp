//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pipeline_test.cpp
//
// Identification: tests/mfabric/pipeline_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string.h>
#include <unistd.h>

#include <assert.h>
#include <stdio.h>
#include <cstring>
#include <cassert>

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/mfabric/libnanomsg/src/nn.h"
#include "backend/mfabric/libnanomsg/src/pipeline.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Pipeline Tests
//===--------------------------------------------------------------------===//

#define NODE0 "node0"
#define NODE1 "node1"

int node0 (const char *url)
{
  int sock = nn_socket (AF_SP, NN_PULL);
  assert (sock >= 0);
  assert (nn_bind (sock, url) >= 0);
  while (1)
    {
      char *buf = NULL;
      int bytes = nn_recv (sock, &buf, NN_MSG, 0);
      assert (bytes >= 0);
      printf ("NODE0: RECEIVED \"%s\"\n", buf);
      nn_freemsg (buf);
    }
}

int node1 (const char *url, const char *msg)
{
  int sz_msg = strlen (msg) + 1; // '\0' too
  int sock = nn_socket (AF_SP, NN_PUSH);
  assert (sock >= 0);
  assert (nn_connect (sock, url) >= 0);
  printf ("NODE1: SENDING \"%s\"\n", msg);
  int bytes = nn_send (sock, msg, sz_msg, 0);
  assert (bytes == sz_msg);
  return nn_shutdown (sock, 0);
}

TEST (PipelineTest, BasicTest)
{
  std::thread send(node0,"ipc:///tmp/pair.ipc");
  send.detach();
  std::thread recv(node1,"ipc:///tmp/pair.ipc","Hello!");
  recv.detach();

  sleep(3);
}

}  // End test namespace
} // End peloton namespace
