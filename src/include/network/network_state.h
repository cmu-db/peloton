//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_state.h
//
// Identification: src/include/network/network_state.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace network {
// Network Thread States
enum class ConnState {
  READ,       // State that reads data from the network
  WRITE,      // State the writes data to the network
  WAIT,       // State for waiting for some event to happen
  PROCESS,    // State that runs the network protocol on received data
  CLOSING,    // State for closing the client connection
  GET_RESULT, // State when triggered by worker thread that completes the task.
};

// TODO(tianyu): Convert use cases of this to just return Transition
enum class WriteState {
  WRITE_COMPLETE,   // Write completed
  WRITE_NOT_READY,  // Socket not ready to write
  WRITE_ERROR,      // Some error happened
};

enum class Transition {
  NONE,
  WAKEUP,
  PROCEED,
  NEED_DATA,
  // TODO(tianyu) generalize this symbol, this is currently only used in process
  GET_RESULT,
  // TODO(tianyu) replace this with exceptions
  ERROR
};


}
}
