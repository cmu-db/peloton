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
#include "network/network_io_wrappers.h"
#include "network_state.h"
#include "protocol_handler.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

namespace peloton {
namespace network {

/**
 * @brief A ConnectionHandle encapsulates all information about a client
 * connection for its entire duration. This includes a state machine and the
 * necessary libevent infrastructure for a handler to work on this connection.
 */
class ConnectionHandle {
 public:
  /**
   * Constructs a new ConnectionHandle
   * @param sock_fd Client's connection fd
   * @param handler The handler responsible for this handle
   */
  ConnectionHandle(int sock_fd, ConnectionHandlerTask *handler);

  /**
   * @brief Signal to libevent that this ConnectionHandle is ready to handle
   * events
   *
   * This method needs to be called separately after initialization for the
   * connection handle to do anything. The reason why this is not performed in
   * the constructor is because it publishes pointers to this object. While the
   * object should be fully initialized at that point, it's never a bad idea
   * to be careful.
   */
  inline void RegisterToReceiveEvents() {
    workpool_event_ = conn_handler_->RegisterManualEvent(
        METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

    // TODO(Tianyi): should put the initialization else where.. check
    // correctness first.
    tcop_.SetTaskCallback(
        [](void *arg) {
          struct event *event = static_cast<struct event *>(arg);
          event_active(event, EV_WRITE, 0);
        },
        workpool_event_);

    network_event_ = conn_handler_->RegisterEvent(
        io_wrapper_->GetSocketFd(), EV_READ | EV_PERSIST,
        METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
  }

  /**
   * Handles a libevent event. This simply delegates the the state machine.
   */
  inline void HandleEvent(int, short) {
    state_machine_.Accept(Transition::WAKEUP, *this);
  }

  /* State Machine Actions */
  // TODO(Tianyu): Write some documentation when feeling like it
  inline Transition TryRead() { return io_wrapper_->FillReadBuffer(); }
  Transition TryWrite();
  Transition Process();
  Transition GetResult();
  Transition TrySslHandshake();
  Transition TryCloseConnection();

  /**
   * Updates the event flags of the network event. This configures how the
   * handler reacts to client activity from this connection.
   * @param flags new flags for the event handle.
   */
  inline void UpdateEventFlags(short flags) {
    conn_handler_->UpdateEvent(
        network_event_, io_wrapper_->GetSocketFd(), flags,
        METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
  }

  /**
   * Stops receiving network events from client connection. This is useful when
   * we are waiting on peloton to return the result of a query and not handling
   * client query.
   */
  inline void StopReceivingNetworkEvent() {
    EventUtil::EventDel(network_event_);
  }

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
  friend class NetworkIoWrapperFactory;

  /**
   * @brief: Determine if there is still responses in the buffer
   * @return true if there is still responses to flush out in either wbuf or
   * responses
   */
  inline bool HasResponse() {
    return (protocol_handler_->responses_.size() != 0) ||
           (io_wrapper_->wbuf_->size_ != 0);
  }

  ConnectionHandlerTask *conn_handler_;
  std::shared_ptr<NetworkIoWrapper> io_wrapper_;
  StateMachine state_machine_;
  struct event *network_event_ = nullptr, *workpool_event_ = nullptr;
  std::unique_ptr<ProtocolHandler> protocol_handler_ = nullptr;
  tcop::TrafficCop tcop_;
  // TODO(Tianyu): Put this into protocol handler in a later refactor
  unsigned int next_response_ = 0;
};
}  // namespace network
}  // namespace peloton
