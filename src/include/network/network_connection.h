//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_connection.h
//
// Identification: src/include/network/network_connection.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/file.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/internal_types.h"

#include "network_thread.h"
#include "network_master_thread.h"
#include "protocol_handler.h"
#include "marshal.h"
#include "network_state.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

namespace peloton {
namespace network {

/*
 * NetworkConnection - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
class NetworkConnection {
 public:
  int thread_id;
  int sock_fd;                    // socket file descriptor
  struct event *network_event = nullptr;  // something to read from network
  struct event *workpool_event = nullptr; // worker thread done the job
  short event_flags;              // event flags mask

  SSL* conn_SSL_context = nullptr;          // SSL context for the connection

  NetworkThread *thread;          // reference to the network thread
  std::unique_ptr<ProtocolHandler> protocol_handler_;       // Stores state for this socket
  ConnState state = ConnState::CONN_INVALID;  // Initial state of connection
  tcop::TrafficCop traffic_cop_;
 private:
  Buffer rbuf_;                     // Socket's read buffer
  Buffer wbuf_;                     // Socket's write buffer
  unsigned int next_response_ = 0;  // The next response in the response buffer
  Client client_;
  bool ssl_handshake_ = false;
  bool finish_startup_packet_ = false;
  InputPacket initial_packet;

  bool ssl_able_;
  // Set when doing rehandshake in SSL
  bool read_blocked_on_write_ = false;
  bool write_blocked_on_read_ = false;
  bool read_blocked_ = false;
  bool write_blocked_ = false;
 public:
  inline NetworkConnection(int sock_fd, short event_flags, NetworkThread *thread,
                        ConnState init_state, bool ssl_able)
      : sock_fd(sock_fd), ssl_able_(ssl_able) {
    Init(event_flags, thread, init_state);
  }

  /* Reuse this object for a new connection. We could be assigned to a
   * new thread, change thread reference.
   */
  void Init(short event_flags, NetworkThread *thread, ConnState init_state);

  /* refill_read_buffer - Used to repopulate read buffer with a fresh
   * batch of data from the socket
   */
  ReadState FillReadBuffer();

  // Transit to the target state
  void TransitState(ConnState next_state);

  // Update the existing event to listen to the passed flags
  bool UpdateEvent(short flags);

  WriteState WritePackets();

  std::string WriteBufferToString();

  void CloseSocket();

  void Reset();

  void SetReadBlockedOnWrite(bool flag) { read_blocked_on_write_ = flag; }

  void SetWriteBlockedOnRead(bool flag) { write_blocked_on_read_ = flag; }

  bool GetReadBlockedOnWrite() { return read_blocked_on_write_; }

  bool GetWriteBlockedOnRead() { return write_blocked_on_read_; }

  void SetReadBlocked(bool flag) { read_blocked_ = flag; }

  void SetWriteBlocked(bool flag) { write_blocked_ = flag; }

  bool GetReadBlocked() { return read_blocked_; }

  bool GetWriteBlocked() { return write_blocked_; }

  static void TriggerStateMachine(void* arg);

  /* Runs the state machine for the protocol. Invoked by event handler callback */
  static void StateMachine(NetworkConnection *conn);

 private:

  ProcessResult ProcessInitial();

  // Extracts the header of a Postgres start up packet from the read socket buffer
  static bool ReadStartupPacketHeader(Buffer &rbuf, InputPacket &rpkt);

  // Writes a packet's header (type, size) into the write buffer
  WriteState BufferWriteBytesHeader(OutputPacket *pkt);

  // Writes a packet's content into the write buffer
  WriteState BufferWriteBytesContent(OutputPacket *pkt);

  // Used to invoke a write into the Socket, returns false if the socket is not
  // ready for write
  WriteState FlushWriteBuffer();

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

};

}
}
