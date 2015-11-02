//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_message.cpp
//
// Identification: src/backend/mfabric/abstract_message.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "abstract_message.h"

namespace peloton {
namespace mfabric {

 /**
  * @brief Creates and returns socket.
  *
  * @return Socket handle for created socket.
  */
  int AbstractMessage::CreateSocket() {

  }

 /**
  * @brief Closes given socket.
  *
  * @return Socket handle for created socket.
  */
  int AbstractMessage::CloseSocket() {

  } 

 /**
  * @brief Binds to a socket.
  *
  * @return True or false based on binding.
  */
  bool AbstractMessage::BindSocket() {

  }

 /**
  * @brief Sends message over the given socket.
  *
  * @return Number of bytes sent over the socket.
  */
  int AbstractMessage::SendMessage() {

  }

  /**
   * @brief Sets given socket with given options.
   *
   * @return Return value indicating success or failure.
   */
  int AbstractMessage::SetSocket() {

  }

  /**
   * @brief Creates a socket with given options.
   *
   * @return Return value indicating success or failure.
   */
  bool AbstractMessage::ConnectSocket() {

  }

  /**
   * @brief receives message
   *
   * @return Return value indicating number of bytes received.
   */
  int AbstractMessage::ReceiveMessage() {

  }

  /**
   * @brief Shuts down socket from further function
   *
   * @return Return value indicating success or failure
   */
  int AbstractMessage::ShutdownSocket() {

  }

}  // namespace mfabric
}  // namespace peloton
