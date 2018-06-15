//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_state.h
//
// Identification: src/include/network/network_state.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace network {
/**
 * States used by ConnectionHandle::StateMachine.
 * @see ConnectionHandle::StateMachine
 */
enum class ConnState {
  READ,      // State that reads data from the network
  WRITE,     // State the writes data to the network
  PROCESS,   // State that runs the network protocol on received data
  SSL_INIT,  // State to flush out responses and doing (Real) SSL handshake
  SHUTDOWN   // Final sink state to close down the connection
};

/**
 * A transition is used to signal the result of an action to
 * ConnectionHandle::StateMachine
 * @see ConnectionHandle::StateMachine
 */
enum class Transition {
  NONE,
  WAKEUP,
  PROCEED,
  NEED_READ,
  NEED_RESULT,
  NEED_SSL_HANDSHAKE,
  NEED_WRITE,
  END
};
}  // namespace network
}  // namespace peloton
