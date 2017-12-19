//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_state_machine.cpp
//
// Identification: src/include/network/network_state_machine.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/connection_handle.h"
#include "network/network_connection.h"


namespace peloton {
namespace network {

/*
 *  Here we are abusing macro expansion to allow for human readable definition of the
 *  finite state machine's transition table. We put these macros in an anonymous
 *  namespace to avoid them confusing people elsewhere.
 *
 *  The macros merely compose a large function. Unless you know what you are doing, do not modify their
 *  definitions.
 *
 *  To use these macros, follow the examples given. Or, here is a syntax chart:
 *
 *  x list ::= x... (separated by new lines)
 *  graph ::= DEF_TRANSITION_GRAPH
 *              state_list
 *            END_DEF

 *  state ::= DEFINE_STATE(ConnState)
 *             transition_list
 *            END_DEF
 *
 *  transition ::= ON (Transition) SET_STATE_TO (ConnState) AND_INVOKE (NetworkConnection method)
 *
 *  Note that all the symbols used must be defined in ConnState, Transition and NetworkConnection, respectively.
 */
namespace {
  // Underneath the hood these macro is defining the static method ConncetionHandleStatMachine::Delta
  // Together they compose a nested switch statement. Running the function on any undefined
  // state or transition on said state will throw a runtime error.
  #define DEF_TRANSITION_GRAPH \
        ConnectionHandleStateMachine::transition_result ConnectionHandleStateMachine::Delta_(ConnState c, Transition t) { \
          switch (c) {
  #define DEFINE_STATE(s) case ConnState::s: { switch (t) {
  #define END_DEF default: throw std::runtime_error("undefined transition"); }}
  #define ON(t) case Transition::t: return
  #define SET_STATE_TO(s) {ConnState::s,
  #define AND_INVOKE(m) ([] (NetworkConnection &w) { return w.m(); })};
  #define AND_WAIT ([] (NetworkConnection &){ return Transition::NONE; })};
}

DEF_TRANSITION_GRAPH
  DEFINE_STATE (CONN_READ)
    ON (WAKEUP) SET_STATE_TO (CONN_READ) AND_INVOKE (FillReadBuffer)
    ON (PROCEED) SET_STATE_TO (CONN_PROCESS) AND_INVOKE (Process)
    ON (NEED_DATA) SET_STATE_TO (CONN_WAIT) AND_INVOKE (Wait)
    ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE (CloseSocket)
  END_DEF

  DEFINE_STATE (CONN_WAIT)
    ON (PROCEED) SET_STATE_TO (CONN_READ) AND_WAIT
    ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE (CloseSocket)
  END_DEF

  DEFINE_STATE (CONN_PROCESS)
    ON (PROCEED) SET_STATE_TO (CONN_WRITE) AND_INVOKE (ProcessWrite)
    ON (NEED_DATA) SET_STATE_TO (CONN_WAIT) AND_INVOKE (Wait)
    ON (GET_RESULT) SET_STATE_TO (CONN_GET_RESULT) AND_WAIT
    ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE (CloseSocket)
  END_DEF

  DEFINE_STATE (CONN_WRITE)
    ON (WAKEUP) SET_STATE_TO (CONN_WRITE) AND_INVOKE (ProcessWrite)
    ON (PROCEED) SET_STATE_TO (CONN_PROCESS) AND_INVOKE (Process)
    ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE(CloseSocket)
  END_DEF

  DEFINE_STATE (CONN_GET_RESULT)
    ON (WAKEUP) SET_STATE_TO (CONN_GET_RESULT) AND_INVOKE (GetResult)
    ON (PROCEED) SET_STATE_TO (CONN_WRITE) AND_INVOKE (ProcessWrite)
  END_DEF
END_DEF

void ConnectionHandleStateMachine::Accept(Transition action, NetworkConnection &connection)   {
  Transition next = action;
  while (next != Transition::NONE) {
    transition_result result = Delta_(current_state_, next);
    current_state_ = result.first;
    next = result.second(connection);
  }
}
}
}