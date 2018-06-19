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
#include "network/network_io_wrapper_factory.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "network/protocol_handler_factory.h"

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
      connection.TryCloseConnection();
      return;
    }
  }
}

ConnectionHandle::ConnectionHandle(int sock_fd, ConnectionHandlerTask *handler)
    : conn_handler_(handler),
      io_wrapper_(NetworkIoWrapperFactory::GetInstance().NewNetworkIoWrapper(sock_fd)) {}

Transition ConnectionHandle::TryWrite() {
  for (; next_response_ < protocol_handler_->responses_.size();
       next_response_++) {
    auto result = io_wrapper_->WritePacket(
        protocol_handler_->responses_[next_response_].get());
    if (result != Transition::PROCEED) return result;
  }
  protocol_handler_->responses_.clear();
  next_response_ = 0;
  if (protocol_handler_->GetFlushFlag()) return io_wrapper_->FlushWriteBuffer();
  protocol_handler_->SetFlushFlag(false);
  return Transition::PROCEED;
}

Transition ConnectionHandle::Process() {
  // TODO(Tianyu): Just use Transition instead of ProcessResult, this looks
  // like a 1 - 1 mapping between the two types.
  if (protocol_handler_ == nullptr)
    // TODO(Tianyi) Check the rbuf here before we create one if we have
    // another protocol handler
    protocol_handler_ = ProtocolHandlerFactory::CreateProtocolHandler(
        ProtocolHandlerType::Postgres, &tcop_);

  ProcessResult status = protocol_handler_->Process(
      *(io_wrapper_->rbuf_), (size_t)conn_handler_->Id());

  switch (status) {
    case ProcessResult::MORE_DATA_REQUIRED:
      return Transition::NEED_READ;
    case ProcessResult::COMPLETE:
      return Transition::PROCEED;
    case ProcessResult::PROCESSING:
      return Transition::NEED_RESULT;
    case ProcessResult::TERMINATE:
      throw NetworkProcessException("Error when processing");
    case ProcessResult::NEED_SSL_HANDSHAKE:
      return Transition::NEED_SSL_HANDSHAKE;
    default:
      LOG_ERROR("Unknown process result");
      throw NetworkProcessException("Unknown process result");
  }
}

Transition ConnectionHandle::GetResult() {
  EventUtil::EventAdd(network_event_, nullptr);
  protocol_handler_->GetResult();
  tcop_.SetQueuing(false);
  return Transition::PROCEED;
}

Transition ConnectionHandle::TrySslHandshake() {
  // Flush out all the response first
  if (HasResponse()) {
    auto write_ret = TryWrite();
    if (write_ret != Transition::PROCEED) return write_ret;
  }
  return NetworkIoWrapperFactory::GetInstance().PerformSslHandshake(
      io_wrapper_);
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
  // This object is essentially managed by libevent (which unfortunately does
  // not accept shared_ptrs.) and thus as we shut down we need to manually
  // deallocate this object.
  delete this;
  return Transition::NONE;
}
}  // namespace network
}  // namespace peloton
