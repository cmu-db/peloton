//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_message.h
//
// Identification: src/backend/mfabric/abstract_message.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace mfabric {

class AbstractMessage {
 public:

  /** @brief CreateSocket function to be overridden by derived class. */
  virtual int CreateSocket();

  /** @brief BindSocket function to be overridden by derived class. */
  virtual bool BindSocket();

  /** @brief SendMessage function to be overridden by derived class. */
  virtual int SendMessage();

  /** @brief SetSocket function to be overridden by derived class. */
  virtual int SetSocket();

  /** @brief ConnectSocket function to be overridden by derived class. */
  virtual bool ConnectSocket();

  /** @brief ReceiveSocket function to be overridden by derived class. */
  virtual int ReceiveMessage();

  /** @brief CloseSocket function to be overridden by derived class. */
  virtual int CloseSocket();

  /** @brief CloseSocket function to be overridden by derived class. */
  virtual int ShutdownSocket();

};

}  // namespace mfabric
}  // namespace peloton
