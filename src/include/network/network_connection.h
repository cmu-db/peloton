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

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/file.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/internal_types.h"

#include "notifiable_task.h"
#include "protocol_handler.h"
#include "marshal.h"
#include "network_state.h"
#include "network/connection_handle.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

namespace peloton {
namespace network {

/*
 * NetworkConnection - Wrapper for managing socket.
 */
class NetworkConnection {
 public:
  int thread_id;
  int sock_fd_;                    // socket file descriptor
  struct event *network_event = nullptr;  // something to read from network
  struct event *workpool_event = nullptr; // worker thread done the job
  short event_flags;              // event flags mask

  SSL* conn_SSL_context = nullptr;          // SSL context for the connection

  NotifiableTask *thread_;          // reference to the network thread
  std::unique_ptr<ProtocolHandler> protocol_handler_;       // Stores state for this socket
  tcop::TrafficCop traffic_cop_;
 private:
  Buffer rbuf_;                     // Socket's read buffer
  Buffer wbuf_;                     // Socket's write buffer
  unsigned int next_response_ = 0;  // The next response in the response buffer
  Client client_;
  
  ConnectionHandleStateMachine state_machine_;
  
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
  inline NetworkConnection(int sock_fd, short event_flags, NotifiableTask *thread,
                         bool ssl_able)
      : sock_fd_(sock_fd), ssl_able_(ssl_able), state_machine_(ConnState::CONN_READ) {
    Init(event_flags, thread);
  }

  /* Reuse this object for a new connection. We could be assigned to a
   * new thread, change thread reference.
   */
  void Init(short event_flags, NotifiableTask *handler);

  /* refill_read_buffer - Used to repopulate read buffer with a fresh
   * batch of data from the socket
   */
  Transition FillReadBuffer();

  // Transit to the target state

  // Update the existing event to listen to the passed flags
  bool UpdateEvent(short flags);

  WriteState WritePackets();

  std::string WriteBufferToString();

  Transition CloseSocket();

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

  inline void TriggerStateMachine() { state_machine_.Accept(Transition::WAKEUP, *this); }

  // Exposed for testing
  const std::unique_ptr<ProtocolHandler> &GetProtocolHandler() const {
    return protocol_handler_;
  }

  /* State Machine Actions*/
  // TODO(tianyu) Maybe move them to their own class
  Transition Wait();
  Transition Process();
  Transition ProcessWrite();
  Transition GetResult();

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

  int sock_fd_;                    // socket file descriptor
  struct event *network_event = nullptr;  // something to read from network
  struct event *workpool_event = nullptr; // worker thread done the job

  SSL* conn_SSL_context = nullptr;          // SSL context for the connection

  NotifiableTask *handler;          // reference to the network thread
  std::unique_ptr<ProtocolHandler> protocol_handler_;       // Stores state for this socket
  tcop::TrafficCop traffic_cop_;

  Buffer rbuf_;                     // Socket's read buffer
  Buffer wbuf_;                     // Socket's write buffer
  unsigned int next_response_ = 0;  // The next response in the response buffer
  Client client_;
  bool ssl_sent_ = false;
  ConnectionHandleStateMachine state_machine_;

};

}
}
