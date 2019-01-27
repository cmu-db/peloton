//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle.cpp
//
// Identification: src/network/connection_handle.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include <cstring>

#include "network/connection_dispatcher_task.h"
#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"
#include "network/peloton_server.h"

#include "common/utility.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace network {

/*
 *  Here we are abusing macro expansion to allow for human readable definition
 *  of the finite state machine's transition table. We put these macros in an
 *  anonymous namespace to avoid them confusing people elsewhere.
 *
 *  The macros merely compose a large function. Unless you know what you are
 *  doing, do not modify their definitions.
 *
 *  To use these macros, follow the examples given. Or, here is a syntax chart:
 *
 *  x list ::= x \n (x list) (each item of x separated by new lines)
 *  graph ::= DEF_TRANSITION_GRAPH
 *              state list
 *            END_DEF
 *
 *  state ::= DEFINE_STATE(ConnState)
 *             transition list
 *            END_STATE_DEF
 *
 *  transition ::=
 *  ON (Transition) SET_STATE_TO (ConnState) AND_INVOKE (ConnectionHandle
 * method)
 *
 *  Note that all the symbols used must be defined in ConnState, Transition and
 *  ClientSocketWrapper, respectively.
 *
 */
namespace {
// Underneath the hood these macro is defining the static method
// ConnectionHandle::StateMachine::Delta.
// Together they compose a nested switch statement. Running the function on any
// undefined state or transition on said state will throw a runtime error.
#define DEF_TRANSITION_GRAPH                                          \
  ConnectionHandle::StateMachine::transition_result                   \
  ConnectionHandle::StateMachine::Delta_(ConnState c, Transition t) { \
    switch (c) {
#define DEFINE_STATE(s) \
  case ConnState::s: {  \
    switch (t) {
#define ON(t)         \
  case Transition::t: \
    return
#define SET_STATE_TO(s) \
  {                     \
    ConnState::s,
#define AND_INVOKE(m)                         \
  ([](ConnectionHandle &w) { return w.m(); }) \
  }                                           \
  ;
#define AND_WAIT_ON_READ                      \
  ([](ConnectionHandle &w) {                  \
    w.UpdateEventFlags(EV_READ | EV_PERSIST); \
    return Transition::NONE;                  \
  })                                          \
  }                                           \
  ;
#define AND_WAIT_ON_WRITE                      \
  ([](ConnectionHandle &w) {                   \
    w.UpdateEventFlags(EV_WRITE | EV_PERSIST); \
    return Transition::NONE;                   \
  })                                           \
  }                                            \
  ;
#define AND_WAIT_ON_PELOTON        \
  ([](ConnectionHandle &w) {       \
    w.StopReceivingNetworkEvent(); \
    return Transition::NONE;       \
  })                               \
  }                                \
  ;
#define END_DEF                                       \
  default:                                            \
    throw std::runtime_error("undefined transition"); \
    }                                                 \
    }

#define END_STATE_DEF \
  ON(TERMINATE) SET_STATE_TO(CLOSING) AND_INVOKE(TryCloseConnection) END_DEF
}  // namespace

// clang-format off
DEF_TRANSITION_GRAPH
    DEFINE_STATE(READ)
        ON(WAKEUP) SET_STATE_TO(READ) AND_INVOKE(TryRead)
        ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
        ON(NEED_READ) SET_STATE_TO(READ) AND_WAIT_ON_READ
        // This case happens only when we use SSL and are blocked on a write
        // during handshake. From peloton's perspective we are still waiting
        // for reads.
        ON(NEED_WRITE) SET_STATE_TO(READ) AND_WAIT_ON_WRITE
    END_STATE_DEF

    DEFINE_STATE(SSL_INIT)
        ON(WAKEUP) SET_STATE_TO(SSL_INIT) AND_INVOKE(TrySslHandshake)
        ON(NEED_READ) SET_STATE_TO(SSL_INIT) AND_WAIT_ON_READ
        ON(NEED_WRITE) SET_STATE_TO(SSL_INIT) AND_WAIT_ON_WRITE
        ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
    END_STATE_DEF

    DEFINE_STATE(PROCESS)
        ON(WAKEUP) SET_STATE_TO(PROCESS) AND_INVOKE(GetResult)
        ON(PROCEED) SET_STATE_TO(WRITE) AND_INVOKE(TryWrite)
        ON(NEED_READ) SET_STATE_TO(READ) AND_INVOKE(TryRead)
        // Client connections are ignored while we wait on peloton
        // to execute the query
        ON(NEED_RESULT) SET_STATE_TO(PROCESS) AND_WAIT_ON_PELOTON
        ON(NEED_SSL_HANDSHAKE) SET_STATE_TO(SSL_INIT) AND_INVOKE(TrySslHandshake)
    END_STATE_DEF

    DEFINE_STATE(WRITE)
        ON(WAKEUP) SET_STATE_TO(WRITE) AND_INVOKE(TryWrite)
        // This happens when doing ssl-rehandshake with client
        ON(NEED_READ) SET_STATE_TO(WRITE) AND_WAIT_ON_READ
        ON(NEED_WRITE) SET_STATE_TO(WRITE) AND_WAIT_ON_WRITE
        ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
    END_STATE_DEF

    DEFINE_STATE(CLOSING)
        ON(WAKEUP) SET_STATE_TO(CLOSING) AND_INVOKE(TryCloseConnection)
        ON(NEED_READ) SET_STATE_TO(WRITE) AND_WAIT_ON_READ
        ON(NEED_WRITE) SET_STATE_TO(WRITE) AND_WAIT_ON_WRITE
    END_STATE_DEF
END_DEF
// clang-format on

void ConnectionHandle::StateMachine::Accept(Transition action,
                                            ConnectionHandle &connection) {
  Transition next = action;
  while (next != Transition::NONE) {
    transition_result result = Delta_(current_state_, next);
    current_state_ = result.first;
    try {
      next = result.second(connection);
    } catch (NetworkProcessException &e) {
      LOG_ERROR("%s\n", e.what());
      next = Transition::TERMINATE;
    }
  }
}

// TODO(Tianyu): Maybe use a factory to initialize protocol_interpreter here
ConnectionHandle::ConnectionHandle(int sock_fd, ConnectionHandlerTask *handler)
    : conn_handler_(handler),
      io_wrapper_{new PosixSocketIoWrapper(sock_fd)},
      protocol_interpreter_{new PostgresProtocolInterpreter(conn_handler_->Id())} {}


Transition ConnectionHandle::GetResult() {
  EventUtil::EventAdd(network_event_, nullptr);
  protocol_interpreter_->GetResult(io_wrapper_->GetWriteQueue());
  return Transition::PROCEED;
}

Transition ConnectionHandle::TrySslHandshake() {
  // TODO(Tianyu): Do we really need to flush here?
  auto ret = io_wrapper_->FlushAllWrites();
  if (ret != Transition::PROCEED) return ret;
  SSL *context;
  if (!io_wrapper_->SslAble()) {
    context = SSL_new(PelotonServer::ssl_context);
    if (context == nullptr)
      throw NetworkProcessException("ssl context for conn failed");
    SSL_set_session_id_context(context, nullptr, 0);
    if (SSL_set_fd(context, io_wrapper_->sock_fd_) == 0)
      throw NetworkProcessException("Failed to set ssl fd");
    io_wrapper_.reset(new SslSocketIoWrapper(std::move(*io_wrapper_), context));
  } else
    context = dynamic_cast<SslSocketIoWrapper *>(io_wrapper_.get())->conn_ssl_context_;

  // The wrapper already uses SSL methods.
  // Yuchen: "Post-connection verification?"
  ERR_clear_error();
  int ssl_accept_ret = SSL_accept(context);
  if (ssl_accept_ret > 0) return Transition::PROCEED;

  int err = SSL_get_error(context, ssl_accept_ret);
  switch (err) {
    case SSL_ERROR_WANT_READ:
      return Transition::NEED_READ;
    case SSL_ERROR_WANT_WRITE:
      return Transition::NEED_WRITE;
    default:
      throw NetworkProcessException("SSL Error, error code" + std::to_string(err));
  }
}

Transition ConnectionHandle::TryCloseConnection() {
  LOG_DEBUG("Attempt to close the connection %d", io_wrapper_->GetSocketFd());
  // TODO(Tianyu): Handle close failure
  Transition close = io_wrapper_->Close();
  if (close != Transition::PROCEED) return close;
  // Remove listening event
  // Only after the connection is closed is it safe to remove events,
  // after this point no object in the system has reference to this
  // connection handle and we will need to destruct and exit.
  conn_handler_->UnregisterEvent(network_event_);
  conn_handler_->UnregisterEvent(workpool_event_);
  return Transition::NONE;
}
}  // namespace network
}  // namespace peloton
