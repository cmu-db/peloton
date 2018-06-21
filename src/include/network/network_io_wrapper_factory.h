//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_io_wrapper_factory.h
//
// Identification: src/include/network/network_io_wrapper_factory.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "network/network_io_wrappers.h"
#include "network/peloton_server.h"

namespace peloton {
namespace network {

/**
 * @brief Factory class for constructing NetworkIoWrapper objects
 * Each NetworkIoWrapper is associated with read and write buffers that are
 * expensive to reallocate on the fly. Thus, instead of destroying these wrapper
 * objects when they are out of scope, we save them until we can transfer their
 * buffers to other wrappers.
 */
// TODO(Tianyu): Make reuse more fine-grained and adjustable
// Currently there is no limit on the number of wrappers we save. This means
// that we never deallocated wrappers unless we shut down. Obviously this will
// be a memory overhead if we had a lot of connections at one point and dropped
// down after a while. Relying on OS fd values for reuse also can backfire. It
// shouldn't be hard to keep a pool of buffers with a size limit instead of a
// bunch of old wrapper objects.
class NetworkIoWrapperFactory {
 public:
  static inline NetworkIoWrapperFactory &GetInstance() {
    static NetworkIoWrapperFactory factory;
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
  std::shared_ptr<NetworkIoWrapper> NewNetworkIoWrapper(int conn_fd);

  /**
   * @brief: process SSL handshake to generate valid SSL
   * connection context for further communications
   * @return FINISH when the SSL handshake failed
   *         PROCEED when the SSL handshake success
   *         NEED_DATA when the SSL handshake is partially done due to network
   *         latency
   */
  Transition PerformSslHandshake(std::shared_ptr<NetworkIoWrapper> &io_wrapper);

 private:
  std::unordered_map<int, std::shared_ptr<NetworkIoWrapper>> reusable_wrappers_;
};
}  // namespace network
}  // namespace peloton
