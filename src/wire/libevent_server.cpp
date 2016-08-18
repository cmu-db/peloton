//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.cpp
//
// Identification: src/wire/libevent_server.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "wire/libevent_server.h"
#include "common/logger.h"
#include "common/macros.h"

namespace peloton {
namespace wire {

void Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                     UNUSED_ATTRIBUTE short what, void *arg) {
  struct event_base *base = (event_base *)arg;
  LOG_INFO("stop");
  event_base_loopexit(base, NULL);
}

void SetTCPNoDelay(evutil_socket_t fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

/**
 * Set a socket to non-blocking mode.
 */
bool SetNonBlocking(int fd) {
  int status = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
  if (status == -1) {
    return false;
  } else {
    return true;
  }
}

void ReadCallback(int fd, UNUSED_ATTRIBUTE short ev, void *arg) {
  SocketManager<PktBuf> *socket_manager = (SocketManager<PktBuf> *)arg;
  if (socket_manager->assigned_to_pkt_manager == false) {
    PacketManager *pkt_manager = new PacketManager(socket_manager);
    socket_manager->socket_pkt_manager.reset(pkt_manager);
    if (!socket_manager->socket_pkt_manager->ManageFirstPacket()) {
      close(fd);
      event_del(socket_manager->ev_read);
      return;
    }
    socket_manager->assigned_to_pkt_manager = true;
  } else {
    if (!socket_manager->socket_pkt_manager->ManagePacket()) {
      close(fd);
      event_del(socket_manager->ev_read);
      return;
    }
  }
}

/**
 * This function will be called by libevent when there is a
 * connpeloton::wire::SocketManagerection
 * ready to be accepted.
 */
void AcceptCallback(struct evconnlistener *listener, evutil_socket_t client_fd,
                    UNUSED_ATTRIBUTE struct sockaddr *address,
                    UNUSED_ATTRIBUTE int socklen, UNUSED_ATTRIBUTE void *ctx) {
  LOG_INFO("New connection on socket %d", int(client_fd));
  // Get the event base
  struct event_base *base = evconnlistener_get_base(listener);

  // Set the client socket to non-blocking mode.
  if (!SetNonBlocking(client_fd))
    LOG_INFO("failed to set client socket non-blocking");
  SetTCPNoDelay(client_fd);

  /* We've accepted a new client, allocate a socket manager to
     maintain the state of this client. */
  SocketManager<PktBuf> *socket_manager = new SocketManager<PktBuf>(client_fd);
  Server::AddSocketManager(socket_manager);

  /* Setup the read event, libevent will call ReadCallback whenever
   * the clients socket becomes read ready.  Make the
   * read event persistent so we don't have to re-add after each
   * read. */
  socket_manager->ev_read = event_new(base, client_fd, EV_READ | EV_PERSIST,
                                      ReadCallback, socket_manager);

  /* Setting up the event does not activate, add the event so it
     becomes active. */
  event_add(socket_manager->ev_read, NULL);
}

Server::Server() {
  struct event_base *base;
  struct evconnlistener *listener;
  struct event *evstop;
  port_ = FLAGS_port;
  max_connections_ = FLAGS_max_connections;

  // For logging purposes
  //  event_enable_debug_mode();
  //  event_set_log_callback(LogCallback);

  // Commented because it's not in the libevent version we're using
  // When we upgrade this should be uncommented
  //  event_enable_debug_logging(EVENT_DBG_ALL);

  // Ignore the broken pipe signal
  // We don't want to exit on write
  // when the client disconnects
  signal(SIGPIPE, SIG_IGN);

  // Create our event base
  base = event_base_new();
  if (!base) {
    LOG_INFO("Couldn't open event base");
    exit(EXIT_FAILURE);
  }
  evstop = evsignal_new(base, SIGHUP, Signal_Callback, base);
  evsignal_add(evstop, NULL);

  if (FLAGS_socket_family == "AF_INET") {
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = INADDR_ANY;
    sin.sin_port = htons(port_);
    listener = evconnlistener_new_bind(
        base, AcceptCallback, NULL, LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
        -1, (struct sockaddr *)&sin, sizeof(sin));
    if (!listener) {
      LOG_INFO("Couldn't create listener");
      exit(EXIT_FAILURE);
    }

    event_base_dispatch(base);

    evconnlistener_free(listener);
    event_free(evstop);
    event_base_free(base);
  }
  // This socket family code is not implemented yet
  else if (FLAGS_socket_family == "AF_UNIX") {
    LOG_INFO("The AF_UNIX socket family is not implemented");
    exit(EXIT_FAILURE);
  }
}

void Server::LogCallback(int severity, UNUSED_ATTRIBUTE const char *msg) {
  UNUSED_ATTRIBUTE const char *s;
  switch (severity) {
    case _EVENT_LOG_DEBUG:
      s = "debug";
      break;
    case _EVENT_LOG_MSG:
      s = "msg";
      break;
    case _EVENT_LOG_WARN:
      s = "warn";
      break;
    case _EVENT_LOG_ERR:
      s = "error";
      break;
    default:
      s = "?";
      break; /* Should not get this far */
  }
  LOG_INFO("[%s] %s\n", s, msg);
}
}
}
