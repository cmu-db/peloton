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
#include "type/types.h"

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
 * 	B is the STL container type used as the protocol's buffer.
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
  bool ssl_sent_ = false;
  ConnectionHandleStateMachine state_machine_;


 public:
  inline NetworkConnection(int sock_fd, short event_flags, NotifiableTask *thread,
                        ConnState init_state)
      : sock_fd_(sock_fd), state_machine_{init_state} {
    Init(event_flags, thread, init_state);
  }

  /* Reuse this object for a new connection. We could be assigned to a
   * new thread, change thread reference.
   */
  void Init(short event_flags, NotifiableTask *thread, ConnState init_state);

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

  inline void TriggerStateMachine() { state_machine_.Accept(Transition::WAKEUP, *this); }

  /* State Machine Actions*/
  // TODO(tianyu) Maybe move them to their own class
  Transition HandleConnListening();
  Transition Wait();
  Transition Process();
  Transition ProcessWrite();
  Transition GetResult();

 private:

  ProcessResult ProcessInitial();

  // Extracts the header of a Postgres start up packet from the read socket buffer
  static bool ReadStartupPacketHeader(Buffer &rbuf, InputPacket &rpkt);

  /* Routine to deal with the first packet from the client */
  bool ProcessInitialPacket(InputPacket* pkt);

  /* Routine to deal with general Startup message */
  bool ProcessStartupPacket(InputPacket* pkt, int32_t proto_version);

  /* Routine to deal with SSL request message */
  bool ProcessSSLRequestPacket(InputPacket *pkt);

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
