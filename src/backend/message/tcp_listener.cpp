//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_network.cpp
//
// Identification: /peloton/src/backend/message/tcp_network.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "tcp_listener.h"
#include "tcp_connection.h"
#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"

#include <functional>

namespace peloton {
namespace message {

void Listener::Run(void* arg) {

    struct sockaddr_in sin;

    /* Clear the sockaddr before using it, in case there are extra
     *          * platform-specific fields that can mess us up. */
    memset(&sin, 0, sizeof(sin));

    /* This is an INET address */
    sin.sin_family = AF_INET;
    /* Listen on 0.0.0.0 */
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    /* Listen on the given port. */
    sin.sin_port = htons(port_);

    listener_ = evconnlistener_new_bind(listen_base_, AcceptConnCb, arg,
        LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE, -1,
        (struct sockaddr*)&sin, sizeof(sin));

    if (!listener_) {
      LOG_ERROR("Couldn't create listener");
      return;
    }

    evconnlistener_set_error_cb(listener_, AcceptErrorCb);

    event_base_dispatch(listen_base_);

    return;
}

void Listener::AcceptConnCb(struct evconnlistener *listener,
        evutil_socket_t fd, struct sockaddr *address, int socklen,
        void *ctx) {

    assert(listener != NULL && address != NULL && socklen >= 0 && ctx != NULL);

    /* We got a new connection! Set up a bufferevent for it. */

    // We should be careful here new connection would lead to memory leak
    // if we don't use shared ptr
    std::shared_ptr<Connection> conn = std::make_shared<Connection>(fd, ctx);

    LOG_INFO ("Server: connection received from fd: %d", fd);

    // prepaere workers
    std::function<void()> worker_conn =
            std::bind(&Connection::Dispatch, conn);

    // add workers to thread pool
    ThreadManager::GetInstance().AddTask(worker_conn);
}

void Listener::AcceptErrorCb(struct evconnlistener *listener, void *ctx) {

    assert(ctx != NULL);

    struct event_base *base = evconnlistener_get_base(listener);
    int err = EVUTIL_SOCKET_ERROR();

    // Debug info
    LOG_ERROR("Got an error %d (%s) on the listener. "
            "Shutting down", err, evutil_socket_error_to_string(err));

    event_base_loopexit(base, NULL);
}

}  // namespace message
}  // namespace peloton
