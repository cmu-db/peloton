//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_manager.h
//
// Identification: src/include/network/network_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace network {
// Network Thread States
enum class ConnState {
  CONN_LISTENING,  // State that listens for new connections
  CONN_READ,       // State that reads data from the network
  CONN_WRITE,      // State the writes data to the network
  CONN_WAIT,       // State for waiting for some event to happen
  CONN_PROCESS,    // State that runs the network protocol on received data
  CONN_CLOSING,    // State for closing the client connection
  CONN_CLOSED,     // State for closed connection
  CONN_INVALID,    // Invalid STate
  CONN_GET_RESULT, // State when triggered by worker thread that completes the task.
};

enum class ReadState {
  READ_DATA_RECEIVED,
  READ_NO_DATA_RECEIVED,
  READ_ERROR,
};

enum class WriteState {
  WRITE_COMPLETE,   // Write completed
  WRITE_NOT_READY,  // Socket not ready to write
  WRITE_ERROR,      // Some error happened
};

}
}
