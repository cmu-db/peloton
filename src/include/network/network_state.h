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
  READ,        // State that reads data from the network
  WRITE,       // State the writes data to the network
  PROCESS,     // State that runs the network protocol on received data
  CLOSING,     // State for closing the client connection
  GET_RESULT,  // State when triggered by worker thread that completes the task.
  PROCESS_WRITE_SSL_HANDSHAKE,  // State to flush out responses and doing (Real)
                                // SSL handshake
};

// TODO(tianyu): Convert use cases of this to just return Transition
enum class WriteState {
  COMPLETE,   // Write completed
  NOT_READY,  // Socket not ready to write
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
  NEED_DATA,
  // TODO(tianyu) generalize this symbol, this is currently only used in process
  GET_RESULT,
  FINISH,
  RETRY,
  NEED_SSL_HANDSHAKE,
};
}  // namespace network
}  // namespace peloton
