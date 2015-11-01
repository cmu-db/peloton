//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nanomsg.h
//
// Identification: src/backend/mfabric/nanomsg.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "abstract_message.h"
#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <nanomsg/pair.h>
#include <nanomsg/pubsub.h>

namespace peloton {
namespace mfabric {

class NanoMsg : public AbstractMessage {
  public:
  /** @brief CreateSocket function nanomsg implementation */
  int CreateSocket(int domain, int protocol);

  /** @brief BindSocket function nanomsg implementation */
  int BindSocket(int socket, const char *address);

  /** @brief SendMessage function nanomsg implementation */
  int SendMessage(int socket, const void *buffer, size_t length, int flags);

  /** @brief SetSocket function nanomsg implementation */
  int SetSocket(int socket, int level, int option, const void *opt_val, size_t opt_val_len);

  /** @brief ConnectSocket function nanomsg implementation */
  int ConnectSocket(int socket, const char *address);

  /** @brief ReceiveSocket function nanomsg implementation */
  int ReceiveMessage(int socket, void *buffer, size_t length, int flags);

  /** @brief CloseSocket function nanomsg implementation */
  int CloseSocket(int socket);

  /** @brief ShutdownSocket function nanomsg implementation */
  int ShutdownSocket(int socket,int how);
};

} // end of mfabric namespace
} // end of peloton namespace
