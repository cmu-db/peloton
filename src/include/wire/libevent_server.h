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
#include "common/exception.h"
#include "container/lock_free_queue.h"
#include <sys/file.h>
#include <fstream>
#include "wire/wire.h"

#define QUEUE_SIZE 100
#define MASTER_THREAD_ID -1

namespace peloton {
namespace wire {

// Libevent Thread States
enum ConnState {
  CONN_LISTENING,  // State that listens for new connections
  CONN_READ,       // State that reads data from the network
  CONN_WRITE,      // State the writes data to the network
  CONN_WAIT,       // State for waiting for some event to happen
  CONN_PROCESS,    // State that runs the wire protocol on received data
  CONN_CLOSING,    // State for closing the client connection
  CONN_CLOSED,     // State for closed connection
  CONN_INVALID,    // Invalid STate
};

enum ReadState {
  READ_DATA_RECEIVED,
  READ_NO_DATA_RECEIVED,
  READ_ERROR,
};

enum WriteState {
  WRITE_COMPLETE,   // Write completed
  WRITE_NOT_READY,  // Socket not ready to write
  WRITE_ERROR,      // Some error happened
};

/* Libevent Callbacks */

/* Used by a worker thread to receive a new connection from the main thread and
 * launch the event handler */
void WorkerHandleNewConn(evutil_socket_t local_fd, short ev_flags, void *arg);

/* Used by a worker to execute the main event loop for a connection */
void EventHandler(evutil_socket_t connfd, short ev_flags, void *arg);

/* Helpers */

/* Helper used by master thread to dispatch new connection to worker thread */
void DispatchConnection(int new_conn_fd, short event_flags);

/* Runs the state machine for the protocol. Invoked by event handler callback */
void StateMachine(LibeventSocket *conn);

// Update event
void UpdateEvent(LibeventSocket *conn, short flags);

/* Set the socket to non-blocking mode */
inline void SetNonBlocking(evutil_socket_t fd) {
  auto flags = fcntl(fd, F_GETFL);
  flags |= O_NONBLOCK;
  if (fcntl(fd, F_SETFL, flags) < 0) {
    LOG_ERROR("Failed to set non-blocking socket");
  }
}

/* Set TCP No Delay for lower latency */
inline void SetTCPNoDelay(evutil_socket_t fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

// Buffers used to batch messages at the socket
struct Buffer {
  size_t buf_ptr;        // buffer cursor
  size_t buf_size;       // buffer size
  size_t buf_flush_ptr;  // buffer cursor for write
  ByteBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0), buf_flush_ptr(0) {
    // capacity of the buffer
    buf.reserve(SOCKET_BUFFER_SIZE);
  }

  inline void Reset() {
    buf_ptr = 0;
    buf_size = 0;
    buf_flush_ptr = 0;
  }

  // single buffer element accessor
  inline uchar GetByte(size_t &index) {
    return buf[index];
  }

  // Get pointer to index location
  inline uchar* GetPtr(size_t index) {
    return &buf[index];
  }

  inline ByteBuf::const_iterator Begin() {
    return std::begin(buf);
  }

  inline ByteBuf::const_iterator End() {
    return std::end(buf);
  }

  inline size_t GetMaxSize() { return SOCKET_BUFFER_SIZE; }
};

struct NewConnQueueItem {
  int new_conn_fd;
  short event_flags;
  ConnState init_state;

  inline NewConnQueueItem(int new_conn_fd, short event_flags,
                          ConnState init_state)
      : new_conn_fd(new_conn_fd),
        event_flags(event_flags),
        init_state(init_state) {}
};

class LibeventThread {
 protected:
  // The connection thread id
  const int thread_id_;
  struct event_base *libevent_base_;

 public:
  LibeventThread(const int thread_id, struct event_base *libevent_base)
      : thread_id_(thread_id), libevent_base_(libevent_base) {
    if (libevent_base_ == nullptr) {
      LOG_ERROR("Can't allocate event base\n");
      exit(1);
    }
  };

  inline struct event_base *GetEventBase() { return libevent_base_; }

  inline int GetThreadID() const { return thread_id_; }

  // TODO implement destructor
  inline ~LibeventThread() {}
};

class LibeventWorkerThread : public LibeventThread {
 private:
  // New connection event
  struct event *new_conn_event_;

 public:
  // Notify new connection pipe(send end)
  int new_conn_send_fd;

  // Notify new connection pipe(receive end)
  int new_conn_receive_fd;

  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;

 public:
  LibeventWorkerThread(const int thread_id);
};

class LibeventMasterThread : public LibeventThread {
 private:
  const int num_threads_;

  // TODO: have a smarter dispatch scheduler
  int next_thread_id_ = 0; // next thread we dispatched to

 public:
  LibeventMasterThread(const int num_threads, struct event_base *libevent_base);

  void DispatchConnection(int new_conn_fd, short event_flags);

  std::vector<std::shared_ptr<LibeventWorkerThread>> &GetWorkerThreads();

  static void StartWorker(peloton::wire::LibeventWorkerThread *worker_thread);
};

/*
 * SocketManager - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
class LibeventSocket {
 public:
  int sock_fd;           // socket file descriptor
  struct event *event;   // libevent handle
  short event_flags;     // event flags mask

  LibeventThread *thread;  // reference to the libevent thread
  PacketManager pkt_manager;  // Stores state for this socket
  ConnState state = CONN_INVALID;  // Initial state of connection
  InputPacket rpkt;        // Used for reading a single Postgres packet

 private:
  Buffer rbuf_;                      // Socket's read buffer
  Buffer wbuf_;                      // Socket's write buffer
  unsigned int next_response_ = 0;  // The next response in the response buffer

 private:
  void Init(short event_flags, LibeventThread *thread, ConnState init_state);

  // Is the requested amount of data available from the current position in
  // the reader buffer?
  bool IsReadDataAvailable(size_t bytes);

  // Parses out packet size from its header
  void GetSizeFromPktHeader(size_t start_index);

 public:
  inline LibeventSocket(int sock_fd, short event_flags, LibeventThread *thread,
                        ConnState init_state)
      : sock_fd(sock_fd) {
    Init(event_flags, thread, init_state);
  }

  /* refill_read_buffer - Used to repopulate read buffer with a fresh
   * batch of data from the socket
   */
  ReadState FillReadBuffer();

  // Transit to the target state
  void TransitState(ConnState next_state);

  // Update the existing event to listen to the passed flags
  bool UpdateEvent(short flags);

  // Extracts the header of a Postgres packet from the read socket buffer
  bool ReadPacketHeader();

  // Extracts the contents of Postgres packet from the read socket buffer
  bool ReadPacket();

  WriteState WritePackets();

  void PrintWriteBuffer();

  void CloseSocket();

  /* Resuse this object for a new connection. We could be assigned to a
   * new thread, change thread reference.
   */
  void Reset(short event_flags, LibeventThread *thread, ConnState init_state);

 private:
  // Writes a packet's header (type, size) into the write buffer
  WriteState BufferWriteBytesHeader(OutputPacket *pkt);

  // Writes a packet's content into the write buffer
  WriteState BufferWriteBytesContent(OutputPacket *pkt);

  // Used to invoke a write into the Socket, returns false if the socket is not
  // ready for write
  WriteState FlushWriteBuffer();
};

struct LibeventServer {
 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;           // port number
  size_t max_connections_;  // maximum number of connections

 public:
  LibeventServer();
  static LibeventSocket *GetConn(const int &connfd);
  static void CreateNewConn(const int &connfd, short ev_flags,
                            LibeventThread *thread, ConnState init_state);

 private:
  /* Maintain a global list of connections.
   * Helps reuse connection objects when possible
   */
  static std::vector<std::unique_ptr<LibeventSocket>> &GetGlobalSocketList();
};
}
}