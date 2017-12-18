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
#include "common/logger.h"

namespace peloton {
  namespace network {

    /*
     *  Here we are abusing macro expansion to allow for human readable definition of the
     *  finite state machine's transition table. We put these macros in an anonymous
     *  namespace to avoid them confusing people elsewhere.
     *
     *  The macros merely compose a large initializer list to define the unordered_map that
     *  is the transition table. Unless you know what you are doing, do not modify their
     *  definitions.
     *
     *  To use these macros, follow the examples given. Note that all the symbols used must
     *  be defined in ConnState, Transition and NetworkStateMachineActions, respectively.
     */
    namespace {
      #define TRANSITION_GRAPH \
        typename ConnectionHandleStateMachine::transition_graph ConnectionHandleStateMachine::delta_ =
      #define DEFINE_STATE(s) {ConnState::s, {
      #define END_DEF }}
      #define ON(t) {Transition::t,
      #define SET_STATE_TO(s) {ConnState::s,
      #define AND_INVOKE(m) ([] (NetworkConnection &w) { return w.m(); })}}
      #define AND_WAIT [] (NetworkConnection &){ return Transition::NONE; }}}
    }

    TRANSITION_GRAPH {
        DEFINE_STATE (CONN_LISTENING)
          ON (WAKEUP) SET_STATE_TO (CONN_LISTENING) AND_INVOKE (HandleConnListening)
        END_DEF,

        DEFINE_STATE (CONN_READ)
          ON (WAKEUP) SET_STATE_TO (CONN_READ) AND_INVOKE (FillReadBuffer),
          ON (PROCEED) SET_STATE_TO (CONN_PROCESS) AND_INVOKE (Process),
          ON (NEED_DATA) SET_STATE_TO (CONN_WAIT) AND_INVOKE (Wait),
          ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE (CloseSocket)
        END_DEF,

        DEFINE_STATE (CONN_WAIT)
          ON (PROCEED) SET_STATE_TO (CONN_READ) AND_WAIT,
          ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE (CloseSocket)
        END_DEF,

        DEFINE_STATE (CONN_PROCESS)
          ON (PROCEED) SET_STATE_TO (CONN_WRITE) AND_INVOKE (ProcessWrite),
          ON (NEED_DATA) SET_STATE_TO (CONN_WAIT) AND_INVOKE (Wait),
          ON (GET_RESULT) SET_STATE_TO (CONN_GET_RESULT) AND_INVOKE (GetResult),
          ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE (CloseSocket)
        END_DEF,

        DEFINE_STATE (CONN_WRITE)
          ON (PROCEED) SET_STATE_TO (CONN_PROCESS) AND_INVOKE (Process),
          ON (ERROR) SET_STATE_TO (CONN_CLOSING) AND_INVOKE(CloseSocket)
        END_DEF,

        DEFINE_STATE (CONN_GET_RESULT)
          ON (PROCEED) SET_STATE_TO (CONN_WRITE) AND_INVOKE (ProcessWrite)
        END_DEF
    };

    void ConnectionHandleStateMachine::Accept(Transition action, NetworkConnection &connection) {
      PL_ASSERT(!(delta_.find(current_state_) == delta_.end())
                && !(delta_[current_state_].find(action) == delta_[current_state_].end()));
      Transition next = action;
      while (next != Transition::NONE) {
        transition_result result = delta_[current_state_][next];
        current_state_ = result.first;
        next = result.second(connection);
      }
    }

  }
}