//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcp_listener.cpp
//
// Identification: src/backend/networking/tcp_listener.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tcp_listener.h"
#include "tcp_connection.h"
#include "rpc_type.h"
#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"
#include "backend/networking/connection_manager.h"

#include <event2/thread.h>
#include <pthread.h>

#include <functional>

namespace peloton {
namespace networking {

Listener::Listener(int port)
    : port_(port), listen_base_(event_base_new()), listener_(NULL) {
  // make libevent support multiple threads (pthread)
  // TODO: put evthread_use_pthreads before event_base_new()?

  assert(listen_base_ != NULL);
  assert(port_ > 0 && port_ < 65535);
}

Listener::~Listener() {
  if (listener_ != NULL) {
    evconnlistener_free(listener_);
  }

  if (listen_base_ != NULL) {
    event_base_free(listen_base_);
  }
}

/*
 * @breif Run is invoked by server. It uses libevent to listen
 *        When a new connection is accepted, AcceptConnCb is invoked
 *        to process the new connection
 * @pram  arg is the rpc_server pointer
 */
void Listener::Run(void *arg) {
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

  /* We must specify this function if we use multiple threads*/
  evthread_use_pthreads();

  // TODO: LEV_OPT_THREADSAFE is necessary here?
  listener_ = evconnlistener_new_bind(
      listen_base_, AcceptConnCb, arg,
      LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE | LEV_OPT_THREADSAFE, -1,
      (struct sockaddr *)&sin, sizeof(sin));

  if (!listener_) {
    LOG_ERROR("Couldn't create listener");
    return;
  }

  evconnlistener_set_error_cb(listener_, AcceptErrorCb);

  event_base_dispatch(listen_base_);

  /* These are called after dispatch execution. */
  evconnlistener_free(listener_);
  event_base_free(listen_base_);

  LOG_TRACE("Serving is done");
  return;
}

/*
 * @breif AcceptConnCb processes the new connection.
 *        First it new a connection with the passing by socket and ctx
 *        where ctx is passed by Run which is rpc_server pointer
 */
void Listener::AcceptConnCb(struct evconnlistener *listener, evutil_socket_t fd,
                            struct sockaddr *address,
                            __attribute__((unused)) int socklen, void *ctx) {
  assert(listener != NULL && address != NULL && socklen >= 0 && ctx != NULL);

  LOG_INFO("Server: connection received");

  /* We got a new connection! Set up a bufferevent for it. */
  struct event_base *base = evconnlistener_get_base(listener);

  NetworkAddress addr(*address);

  /* Each connection has a bufferevent which is use to recv and send data*/
  Connection *conn = new Connection(fd, base, ctx, addr);

  /* The connection is added in the conn pool, which can be used in the future*/
  ConnectionManager::GetInstance().AddConn(*address, conn);

  LOG_INFO("Server: connection received from fd: %d, address: %s, port:%d", fd,
           addr.IpToString().c_str(), addr.GetPort());
}

void Listener::AcceptErrorCb(struct evconnlistener *listener,
                             __attribute__((unused)) void *ctx) {
  assert(ctx != NULL);

  struct event_base *base = evconnlistener_get_base(listener);

  // Debug info
  LOG_ERROR(
      "Got an error %d (%s) on the listener. "
      "Shutting down",
      EVUTIL_SOCKET_ERROR(),
      evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

  event_base_loopexit(base, NULL);
}

}  // namespace networking
}  // namespace peloton
