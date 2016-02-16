//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_message.h
//
// Identification: src/backend/message/abstract_message.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace message {

class AbstractMessage {
 public:
  /** @brief CreateSocket function to be overridden by derived class. */
  virtual int CreateSocket() = 0;

  /** @brief BindSocket function to be overridden by derived class. */
  virtual bool BindSocket() = 0;

  /** @brief SendMessage function to be overridden by derived class. */
  virtual int SendMessage() = 0;

  /** @brief SetSocket function to be overridden by derived class. */
  virtual int SetSocket() = 0;

  /** @brief ConnectSocket function to be overridden by derived class. */
  virtual bool ConnectSocket() = 0;

  /** @brief ReceiveSocket function to be overridden by derived class. */
  virtual int ReceiveMessage() = 0;

  /** @brief CloseSocket function to be overridden by derived class. */
  virtual int CloseSocket() = 0;

  /** @brief CloseSocket function to be overridden by derived class. */
  virtual int ShutdownSocket() = 0;

  virtual ~AbstractMessage(){};
};

}  // namespace message
}  // namespace peloton
