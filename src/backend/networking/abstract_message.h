//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_message.h
//
// Identification: src/backend/networking/abstract_message.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdlib.h>

namespace peloton {
namespace networking {

class AbstractMessage {
 public:
  /** @brief BindSocket function nanomsg implementation */
  int Bind(const char *address);

  /** @brief SetSocketOpt function nanomsg implementation */
  int SetSocketOpt(int level, int option, const void *opt_val,
                   size_t opt_val_len);

  /** @brief GetSocketOpt function nanomsg implementation */
  int GetSocketOpt(int level, int option, void *opt_val, size_t *opt_val_len);

  /** @brief Connect function nanomsg implementation */
  int Connect(const char *address);

  /** @brief Send function nanomsg implementation */
  int Send(const void *buffer, size_t length, int flags);

  /** @brief Receive function nanomsg implementation */
  int Receive(void *buffer, size_t length, int flags);

  /** @brief Shutdown function nanomsg implementation */
  void Shutdown(int how);
};

}  // namespace networking
}  // namespace peloton
