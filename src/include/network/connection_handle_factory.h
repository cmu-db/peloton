//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle_factory.h
//
// Identification: src/include/network/connection_handle_factory.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "network/connection_handle.h"
#include "network/peloton_server.h"

namespace peloton {
namespace network {

/**
 * @brief Factory class for constructing ConnectionHandle objects
 * Each ConnectionHandle is associated with read and write buffers that are
 * expensive to reallocate on the fly. Thus, instead of destroying these wrapper
 * objects when they are out of scope, we save them until we can transfer their
 * buffers to other wrappers.
 */
// TODO(Tianyu): Additionally, it is hard to make sure the ConnectionHandles
// don't leak without this factory since they are essentially managed by
// libevent if nothing in our system holds reference to them, and libevent
// doesn't cleanup raw pointers.
class ConnectionHandleFactory {
 public:
  static inline ConnectionHandleFactory &GetInstance() {
    static ConnectionHandleFactory factory;
    return factory;
  }

  /**
   * @brief Creates or re-purpose a NetworkIoWrapper object for new use.
   * The returned value always uses Posix I/O methods unles explicitly
   * converted.
   * @see NetworkIoWrapper for details
   * @param conn_fd Client connection fd
   * @return A new NetworkIoWrapper object
   */
  ConnectionHandle &NewConnectionHandle(int conn_fd, ConnectionHandlerTask *task);

 private:
  std::unordered_map<int, ConnectionHandle> reusable_handles_;
};
}  // namespace network
}  // namespace peloton
