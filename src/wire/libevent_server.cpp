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
#include <fstream>
#include "wire/libevent_server.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/init.h"
#include "common/thread_pool.h"

namespace peloton {
namespace wire {

std::vector<SocketManager<PktBuf> *> Server::socket_manager_vector_ = {};
unsigned int Server::socket_manager_id = 0;

/**
 * Stop signal handling
 */
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

/**
 * Process refill the buffer and process all packets that can be processed
 */
void ManageRead(SocketManager<PktBuf> **socket_manager) {
  // Startup packet
  if ((*socket_manager)->first_packet == false) {
    if (!(*socket_manager)->socket_pkt_manager->ManageFirstPacket()) {
      close((*socket_manager)->GetSocketFD());
      event_del((*socket_manager)->ev_read);
      (*socket_manager)->execution_mutex.unlock();
      return;
    }
    (*socket_manager)->first_packet = true;
  }
  // Regular packet
  else {
    if (!(*socket_manager)->socket_pkt_manager->ManagePacket()) {
      close((*socket_manager)->GetSocketFD());
      event_del((*socket_manager)->ev_read);
      (*socket_manager)->execution_mutex.unlock();
      return;
    }
  }
  // Unlock the socket manager mutex
  (*socket_manager)->execution_mutex.unlock();
}

/**
 * The function called when there is new data ready to be read
 */
void ReadCallback(UNUSED_ATTRIBUTE int fd, UNUSED_ATTRIBUTE short ev,
                  void *arg) {
  // Assign a thread if the socket manager is not executing
  if (((SocketManager<PktBuf> *)arg)->execution_mutex.try_lock()) {
    ((SocketManager<PktBuf> *)arg)->self = (SocketManager<PktBuf> *)arg;
    thread_pool.SubmitTask(ManageRead, &((SocketManager<PktBuf> *)arg)->self);
  }
}

/**
 * This function will be called by libevent when there is a connection
 * ready to be accepted.
 */
void AcceptCallback(struct evconnlistener *listener, evutil_socket_t client_fd,
                    UNUSED_ATTRIBUTE struct sockaddr *address,
                    UNUSED_ATTRIBUTE int socklen, UNUSED_ATTRIBUTE void *ctx) {
  LOG_INFO("New connection on fd %d", int(client_fd));
  // Get the event base
  struct event_base *base = evconnlistener_get_base(listener);

  SetTCPNoDelay(client_fd);
  /* We've accepted a new client, allocate a socket manager to
     maintain the state of this client. */
  SocketManager<PktBuf> *socket_manager =
      new SocketManager<PktBuf>(client_fd, ++Server::socket_manager_id);
  socket_manager->socket_pkt_manager.reset(new PacketManager(socket_manager));

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
  socket_manager_id = 0;
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
  // Add hang up signal event
  evstop = evsignal_new(base, SIGHUP, Signal_Callback, base);
  evsignal_add(evstop, NULL);

  if (FLAGS_socket_family == "AF_INET") {
    struct sockaddr_in sin;
    PL_MEMSET(&sin, 0, sizeof(sin));
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
    struct sockaddr_un serv_addr;
    int len;

    std::string SOCKET_PATH = "/tmp/.s.PGSQL." + std::to_string(port_);
    PL_MEMSET(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    strncpy(serv_addr.sun_path, SOCKET_PATH.c_str(),
            sizeof(serv_addr.sun_path) - 1);
    unlink(serv_addr.sun_path);
    len = strlen(serv_addr.sun_path) + sizeof(serv_addr.sun_family);

    listener = evconnlistener_new_bind(
        base, AcceptCallback, NULL, LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
        -1, (struct sockaddr *)&serv_addr, len);
    if (!listener) {
      LOG_INFO("Couldn't create listener");
      exit(EXIT_FAILURE);
    }

    event_base_dispatch(base);

    evconnlistener_free(listener);
    event_free(evstop);
    event_base_free(base);

  } else {
    LOG_ERROR("Socket family %s not supported", FLAGS_socket_family.c_str());
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
