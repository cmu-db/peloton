//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire.h
//
// Identification: src/include/wire/libevent_thread.h
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
#include "container/lock_free_queue.h"
#include <sys/file.h>
#include <fstream>
#include "wire/wire.h"

#define SOCKET_BUFFER_SIZE 8192
#define QUEUE_SIZE 100

namespace peloton {
namespace wire {

/* Libevent Callbacks */

/* Used by a worker thread to receive a new connection from the main thread and
 * launch the event handler */
void WorkerHandleNewConn(evutil_socket_t local_fd, short ev_flags, void *arg);

/* Used by a worker to execute the main event loop for a connection */
void EventHandler(evutil_socket_t connfd, short ev_flags, void *arg);

// Buffers used to batch messages at the socket
struct Buffer {
  size_t buf_ptr;   // buffer cursor
  size_t buf_size;  // buffer size
  SockBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0) {}

  inline void Reset() {
    buf_ptr = 0;
    buf_size = 0;
  }

  inline size_t GetMaxSize() { return SOCKET_BUFFER_SIZE; }
};

struct NewConnQueueItem {
  int new_conn_fd;
  // enum conn_states init_state;
  short event_flags;
};


class LibeventThread {

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

  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;

 public:
  inline ~LibeventThread() {}

  static void ProcessConnRequest(int num);

  static void MainLoop(void *arg);

  LibeventThread() : new_conn_queue(QUEUE_SIZE) {}
};


/*
 * SocketManager - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
class LibeventSocket {
 public:
  int sock_fd;  // socket file descriptor
  bool is_disconnected; // is the connection disconnected
  struct event *event; // libevent handle
  short event_flags;  // event flags mask
  Buffer rbuf;  // Socket's read buffer
  Buffer wbuf;  // Socket's write buffer
  LibeventThread *thread; // reference to the libevent thread
  std::unique_ptr<PacketManager> pkt_manager; // Stores state for this socket

 private:
  /* refill_read_buffer - Used to repopulate read buffer with a fresh
  * batch of data from the socket
  */
  bool RefillReadBuffer();

  inline void Init(short event_flags, LibeventThread *thread) {
    is_disconnected = false;
    this->event_flags = event_flags;
    this->thread = thread;
    event = event_new(thread->libevent_base_, sock_fd, event_flags, EventHandler, nullptr);
    event_add(event, nullptr);
  }

 public:
  inline LibeventSocket(int sock_fd, short event_flags,
                        LibeventThread *thread) :
      sock_fd(sock_fd), event_flags(event_flags), thread(thread) {
    Init(event_flags, thread);
  }


  // Reads a packet of length "bytes" from the head of the buffer
  bool ReadBytes(PktBuf &pkt_buf, size_t bytes);

  // Writes a packet into the write buffer
  bool BufferWriteBytes(PktBuf &pkt_buf, size_t len, uchar type);

  void PrintWriteBuffer();

  // Used to invoke a write into the Socket, once the write buffer is ready
  bool FlushWriteBuffer();

  void CloseSocket();

  /* Resuse this object for a new connection. We could be assigned to a
   * new thread, change thread reference.
   */
  void Reset(short event_flags, LibeventThread *thread) {
    is_disconnected = false;
    rbuf.Reset();
    wbuf.Reset();
    pkt_manager.reset(nullptr);
    Init(event_flags, thread);
  }
};

struct LibeventServer {
 private:
  // For logging purposes
  static void LogCallback(int severity, const char* msg);
  uint64_t port_;             // port number
  size_t max_connections_;  // maximum number of connections

 public:
  LibeventServer();
  static LibeventSocket* GetConn(const int& connfd);
  static void CreateNewConn(const int& connfd, short ev_flags, LibeventThread *thread);

 private:
  /* Maintain a global list of connections.
   * Helps reuse connection objects when possible
   */
  static std::vector<std::unique_ptr<LibeventSocket>>& GetGlobalSocketList();

};

}
}
