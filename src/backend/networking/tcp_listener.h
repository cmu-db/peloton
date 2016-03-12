//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_network.h
//
// Identification: /peloton/src/backend/networking/tcp_listener.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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

class Listener {
public:

    Listener(int port) :
        //server_threads_(1),
        listen_base_(event_base_new()),
        listener_(NULL),
        port_(port) {

        assert(listen_base_ != NULL);
        assert(port_ > 0 && port_ < 65535);
    }

    ~Listener() {

        if (listener_ != NULL) {
            evconnlistener_free(listener_);
        }

        if (listen_base_ != NULL) {
            event_base_free(listen_base_);
        }
    }

    int GetPort() const { return port_; }

    event_base* GetEventBase() const { return listen_base_; }

    evconnlistener* GetListener() const { return listener_; }

    // Begin listening on port.
    void Run(void* arg);

    //ThreadManager server_threads_;

private:
    static void AcceptConnCb(struct evconnlistener *listener,
            evutil_socket_t fd, struct sockaddr *address, int socklen,
            void *ctx);

    static void AcceptErrorCb(struct evconnlistener *listener,
            void *ctx);

//    static void echo_read_cb(struct bufferevent *bev, void *ctx);

//    static void echo_event_cb(struct bufferevent *bev, short events, void *ctx);

    event_base* listen_base_;
    evconnlistener* listener_;
    int port_;
    //TCPListenerCallback* target_;

    event* event_;
};


}  // namespace networking
}  // namespace peloton


