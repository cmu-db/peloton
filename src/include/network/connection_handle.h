//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle.h
//
// Identification: src/include/network/connection_handle.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <unordered_map>

#include "network_state.h"
#include "common/logger.h"

namespace peloton{
namespace network {

// TODO(tianyu): remove this front declaration once we breakup NetworkConnection
class NetworkConnection;

class ConnectionHandleStateMachine {
public:
  /**
   * A state machine is defined to be a set of states, a set of symbols it supports, and a function mapping each
   * state and symbol pair to the state it should transition to. i.e. transition_graph = state * symbol -> state
   *
   * In addition to the transition system, our network state machine also needs to perform actions. Actions are
   * defined as functions (lambdas, or closures, in various other languages) and is promised to be invoked by the
   * state machine after each transition if registered in the transition graph.
   *
   * So the transition graph overall has type transition_graph = state * symbol -> state * action
   */
  using action = Transition (*)(NetworkConnection &);
  using transition_result = std::pair<ConnState, action>;

  explicit ConnectionHandleStateMachine(ConnState state): current_state_(state) {}

  /**
   * Runs the internal state machine, starting from the symbol given, until no more
   * symbols are available.
   *
   * Each state of the state machine defines a map from a transition symbol to an action
   * and the next state it should go to. The actions can either generate the next symbol,
   * which means the state machine will continue to run on the generated symbol, or signal
   * that there is no more symbols that can be generated, at which point the state machine
   * will stop running and return, waiting for an external event (user interaction, or system event)
   * to generate the next symbol.
   *
   * @param action starting symbol
   * @param connection the network connection object to apply actions to
   */
  void Accept(Transition action, NetworkConnection &connection);

private:
  /**
   * delta is the transition function that defines, for each state, its behavior and the
   * next state it should go to.
   */
  static transition_result Delta_(ConnState state, Transition transition);
  ConnState current_state_;
};

}
}
