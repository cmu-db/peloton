//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_io_wrapper_factory.cpp
//
// Identification: src/network/network_io_wrapper_factory.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "network/network_io_wrapper_factory.h"

namespace peloton {
namespace network {
std::shared_ptr<NetworkIoWrapper> NetworkIoWrapperFactory::NewNetworkIoWrapper(
    int conn_fd) {
  auto it = reusable_wrappers_.find(conn_fd);
  if (it == reusable_wrappers_.end()) {
    // No reusable wrappers
    auto wrapper = std::make_shared<PosixSocketIoWrapper>(
        conn_fd, std::make_shared<ReadBuffer>(),
        std::make_shared<WriteBuffer>());
    reusable_wrappers_[conn_fd] =
        std::static_pointer_cast<NetworkIoWrapper, PosixSocketIoWrapper>(
            wrapper);
    return wrapper;
  }

  // Construct new wrapper by reusing buffers from the old one.
  // The old one will be deallocated as we replace the last reference to it
  // in the reusable_wrappers_ map. We still need to explicitly call the
  // constructor so the flags are set properly on the new file descriptor.
  auto &reused_wrapper = it->second;
  reused_wrapper = std::make_shared<PosixSocketIoWrapper>(conn_fd,
                                                          reused_wrapper->rbuf_,
                                                          reused_wrapper->wbuf_);
  return reused_wrapper;
}

Transition NetworkIoWrapperFactory::PerformSslHandshake(
    std::shared_ptr<NetworkIoWrapper> &io_wrapper) {
  SSL *context;
  if (!io_wrapper->SslAble()) {
    context = SSL_new(PelotonServer::ssl_context);
    if (context == nullptr)
      throw NetworkProcessException("ssl context for conn failed");
    SSL_set_session_id_context(context, nullptr, 0);
    if (SSL_set_fd(context, io_wrapper->sock_fd_) == 0)
      throw NetworkProcessException("Failed to set ssl fd");
    io_wrapper =
        std::make_shared<SslSocketIoWrapper>(std::move(*io_wrapper), context);
  } else {
    auto ptr = std::dynamic_pointer_cast<SslSocketIoWrapper, NetworkIoWrapper>(
        io_wrapper);
    context = ptr->conn_ssl_context_;
  }

  // The wrapper already uses SSL methods.
  // Yuchen: "Post-connection verification?"
  ERR_clear_error();
  int ssl_accept_ret = SSL_accept(context);
  if (ssl_accept_ret > 0) return Transition::PROCEED;

  int err = SSL_get_error(context, ssl_accept_ret);
  switch (err) {
    case SSL_ERROR_WANT_READ:
      return Transition::NEED_READ;
    case SSL_ERROR_WANT_WRITE:
      return Transition::NEED_WRITE;
    default:
      throw NetworkProcessException("SSL Error, error code" + std::to_string(err));
  }
}
}  // namespace network
}  // namespace peloton