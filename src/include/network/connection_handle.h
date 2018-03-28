//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle.h
//
// Identification: src/include/network/connection_handle.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <unordered_map>

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

#include "marshal.h"
#include "network/connection_handler_task.h"
#include "network_state.h"
#include "protocol_handler.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

namespace peloton {
namespace network {

// TODO(tianyu) This class is not refactored in full as rewriting the logic is
// not cost-effective. However, readability
// improvement and other changes may become desirable in the future. Other than
// code clutter, responsibility assignment
// is not well thought-out in this class. Abstracting out some type of socket
// wrapper would be nice.
/**
 * @brief A ConnectionHandle encapsulates all information about a client
 * connection for its entire duration.
 * One should not use the constructor to construct a new ConnectionHandle
 * instance every time as it is expensive
 * to allocate buffers. Instead, use the ConnectionHandleFactory.
 *
 * @see ConnectionHandleFactory
 */
class ConnectionHandle {
 public:
  /**
   * Update the existing event to listen to the passed flags
   */
  void UpdateEventFlags(short flags);

  WriteState WritePackets();

  std::string WriteBufferToString();

  inline void HandleEvent(int, short) {
    state_machine_.Accept(Transition::WAKEUP, *this);
  }

  // Exposed for testing
  const std::unique_ptr<ProtocolHandler> &GetProtocolHandler() const {
    return protocol_handler_;
  }

  // State Machine actions
  /**
   * refill_read_buffer - Used to repopulate read buffer with a fresh
   * batch of data from the socket
   */
  Transition FillReadBuffer();
  Transition Wait();
  Transition Process();
  Transition ProcessWrite();
  Transition GetResult();
  Transition CloseSocket();
  /**
   * Flush out all the responses and do real SSL handshake
   */
  Transition ProcessWrite_SSLHandshake();

 private:
  /**
   * A state machine is defined to be a set of states, a set of symbols it
   * supports, and a function mapping each
   * state and symbol pair to the state it should transition to. i.e.
   * transition_graph = state * symbol -> state
   *
   * In addition to the transition system, our network state machine also needs
   * to perform actions. Actions are
   * defined as functions (lambdas, or closures, in various other languages) and
   * is promised to be invoked by the
   * state machine after each transition if registered in the transition graph.
   *
   * So the transition graph overall has type transition_graph = state * symbol
   * -> state * action
   */
  class StateMachine {
   public:
    using action = Transition (*)(ConnectionHandle &);
    using transition_result = std::pair<ConnState, action>;
    /**
     * Runs the internal state machine, starting from the symbol given, until no
     * more
     * symbols are available.
     *
     * Each state of the state machine defines a map from a transition symbol to
     * an action
     * and the next state it should go to. The actions can either generate the
     * next symbol,
     * which means the state machine will continue to run on the generated
     * symbol, or signal
     * that there is no more symbols that can be generated, at which point the
     * state machine
     * will stop running and return, waiting for an external event (user
     * interaction, or system event)
     * to generate the next symbol.
     *
     * @param action starting symbol
     * @param connection the network connection object to apply actions to
     */
    void Accept(Transition action, ConnectionHandle &connection);

   private:
    /**
     * delta is the transition function that defines, for each state, its
     * behavior and the
     * next state it should go to.
     */
    static transition_result Delta_(ConnState state, Transition transition);
    ConnState current_state_ = ConnState::READ;
  };

  friend class StateMachine;
  friend class ConnectionHandleFactory;

  ConnectionHandle(int sock_fd, ConnectionHandlerTask *handler,
                   std::shared_ptr<Buffer> rbuf, std::shared_ptr<Buffer> wbuf);

  /**
   * Writes a packet's header (type, size) into the write buffer
   */
  WriteState BufferWriteBytesHeader(OutputPacket *pkt);

  /**
   * Writes a packet's content into the write buffer
   */
  WriteState BufferWriteBytesContent(OutputPacket *pkt);

  /**
   * Used to invoke a write into the Socket, returns false if the socket is not
   * ready for write
   */
  WriteState FlushWriteBuffer();

  /**
   * @brief: process SSL handshake to generate valid SSL
   * connection context for further communications
   * @return FINISH when the SSL handshake failed
   *         PROCEED when the SSL handshake success
   *         NEED_DATA when the SSL handshake is partially done due to network
   *         latency
   */
  Transition SSLHandshake();

  /**
   * Set the socket to non-blocking mode
   */
  inline void SetNonBlocking(evutil_socket_t fd) {
    auto flags = fcntl(fd, F_GETFL);
    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) < 0) {
      LOG_ERROR("Failed to set non-blocking socket");
    }
  }

  /**
   * Set TCP No Delay for lower latency
   */
  inline void SetTCPNoDelay(evutil_socket_t fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  }

  /**
   * @brief: Determine if there is still responses in the buffer
   * @return true if there is still responses to flush out in either wbuf or
   * responses
   */
  inline bool HasResponse() {
    return (protocol_handler_->responses_.size() != 0) ||
           (wbuf_->buf_size != 0);
  }

  int sock_fd_;                            // socket file descriptor
  struct event *network_event = nullptr;   // something to read from network
  struct event *workpool_event = nullptr;  // worker thread done the job

  SSL *conn_SSL_context = nullptr;  // SSL context for the connection

  ConnectionHandlerTask *handler_;  // reference to the network thread
  std::unique_ptr<ProtocolHandler>
      protocol_handler_;  // Stores state for this socket
  tcop::TrafficCop traffic_cop_;

  std::shared_ptr<Buffer> rbuf_;    // Socket's read buffer
  std::shared_ptr<Buffer> wbuf_;    // Socket's write buffer
  unsigned int next_response_ = 0;  // The next response in the response buffer

  StateMachine state_machine_;

  short curr_event_flag_;  // current libevent event flag
};
}  // namespace network
}  // namespace peloton
