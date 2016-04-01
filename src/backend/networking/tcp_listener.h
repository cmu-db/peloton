//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcp_listener.h
//
// Identification: src/backend/networking/tcp_listener.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/thread_manager.h"

#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>

#include <cassert>

namespace peloton {
namespace networking {
class Connection;
class Listener {
 public:
  // This is the server port
  Listener(int port);
  ~Listener();

  // Get the server port
  int GetPort() const { return port_; }

  // The listenner event is in the listen_base_
  struct event_base* GetEventBase() const {
    return listen_base_;
  }

  // listener is a evconnlistener type which is a libevent type
  struct evconnlistener* GetListener() const {
    return listener_;
  }

  // Begin listening
  void Run(void* arg);

 private:
  // AcceptConnCb is a callback invoked when a new connection is accepted
  static void AcceptConnCb(struct evconnlistener* listener, evutil_socket_t fd,
                           struct sockaddr* address, int socklen, void* ctx);

  // AcceptErrorCb is invoked when error occurs
  static void AcceptErrorCb(struct evconnlistener* listener, void* ctx);

  // server listenning port
  int port_;

  // The listenner event is in the listen_base_
  struct event_base* listen_base_;

  // listener is a evconnlistener type which is a libevent type
  struct evconnlistener* listener_;
};

}  // namespace networking
}  // namespace peloton
