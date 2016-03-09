//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nanomsg.h
//
// Identification: src/backend/message/nanomsg.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <exception>

#include "backend/networking/abstract_message.h"

namespace peloton {
namespace networking {

class NanoMsg : public AbstractMessage {

public:

  /** @brief BindSocket function nanomsg implementation */
  int Bind(__attribute__((unused)) const char *address) {
    return 0;
  }

  /** @brief SetSocketOpt function nanomsg implementation */
  int SetSocketOpt(__attribute__((unused)) int level,
                   __attribute__((unused)) int option,
                   __attribute__((unused)) const void *opt_val,
                   __attribute__((unused)) size_t opt_val_len) {
    return 0;
  }

  /** @brief GetSocketOpt function nanomsg implementation */
  int GetSocketOpt(__attribute__((unused)) int level,
                   __attribute__((unused)) int option,
                   __attribute__((unused)) void *opt_val,
                   __attribute__((unused)) size_t *opt_val_len) {
    return 0;
  }

  /** @brief Connect function nanomsg implementation */
  int Connect(__attribute__((unused)) const char *address) {
    return 0;
  }

  /** @brief Send function nanomsg implementation */
  int Send(__attribute__((unused)) const void *buffer,
           __attribute__((unused)) size_t length,
           __attribute__((unused)) int flags) {
    return 0;
  }

  /** @brief Receive function nanomsg implementation */
  int Receive(__attribute__((unused)) void *buffer,
              __attribute__((unused)) size_t length,
              __attribute__((unused)) int flags) {
    return 0;
  }

  /** @brief Shutdown function nanomsg implementation */
  void Shutdown(__attribute__((unused)) int how) {
  }

private:

  int socket_;

};

}  // end of networking namespace
}  // end of peloton namespace
