//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire.h
//
// Identification: src/include/wire/libevent_server.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/event.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "common/logger.h"
#include "common/config.h"
#include "wire/wire.h"
#include "wire/socket_base.h"
#include "container/lock_free_queue.h"
#include <sys/file.h>
#include <fstream>

namespace peloton {

namespace wire {

class LibeventThread {

 public:
  LibeventThread() {}

  inline ~LibeventThread() {}

  static void ProcessConnRequest(int num);

  static void MainLoop(void *arg);

  // TODO change back to private
 public:
  /* libevent handle this thread uses */
  struct event_base *libevent_base_;

  /* listen event for notify pipe */
  struct event *notify_new_conn_;

  /* receiving end of notify pipe */
  int notify_receive_fd_;

  /* sending end of notify pipe */
  int notify_send_fd_;

  // The queue for new connection requests
  // LockFreeQueue<std::shared_ptr<int64_t >> new_conn_queue;
};
}
}
