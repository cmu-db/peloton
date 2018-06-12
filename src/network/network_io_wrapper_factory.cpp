//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle_factory.cpp
//
// Identification: src/network/connection_handle_factory.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/network_io_wrapper_factory.h"

namespace peloton {
namespace network {
std::shared_ptr<NetworkIoWrapper> NetworkIoWrapperFactory::NewNetworkIoWrapper(
    int conn_fd) {
  auto it = reusable_wrappers_.find(conn_fd);
  if (it == reusable_wrappers_.end()) {
    // No reusable wrappers
    auto wrapper = std::make_shared<PosixSocketIoWrapper>(conn_fd,
                                                          std::make_shared<
                                                              ReadBuffer>(),
                                                          std::make_shared<
                                                              WriteBuffer>());
    reusable_wrappers_[conn_fd] =
        std::static_pointer_cast<NetworkIoWrapper, PosixSocketIoWrapper>(
            wrapper);
    return wrapper;
  }

  // Construct new wrapper by reusing buffers from the old one.
  // The old one will be deallocated as we replace the last reference to it
  // in the reusable_wrappers_ map
  auto reused_wrapper = it->second;
  reused_wrapper->rbuf_->Reset();
  reused_wrapper->wbuf_->Reset();
  reused_wrapper->sock_fd_ = conn_fd;
  reused_wrapper->conn_ssl_context_ = nullptr;
  // It is not necessary to have an explicit cast here because the reused
  // wrapper always use Posix methods, as we never update their type in the
  // reusable wrappers map.
  auto new_wrapper = new std::shared_ptr<NetworkIoWrapper>(
      reinterpret_cast<PosixSocketIoWrapper *>(reused_wrapper.get()));
  return reused_wrapper;
}

Transition NetworkIoWrapperFactory::PerformSslHandshake(std::shared_ptr<
    NetworkIoWrapper> &io_wrapper) {
  if (io_wrapper->conn_ssl_context_ == nullptr) {
    // Initial handshake, the incoming type is a posix socket wrapper
    auto *context = io_wrapper->conn_ssl_context_ =
                        SSL_new(PelotonServer::ssl_context);
    // TODO(Tianyu): Is it the right thing here to throw exceptions?
    if (context == nullptr)
      throw NetworkProcessException("ssl context for conn failed");
    SSL_set_session_id_context(context, nullptr, 0);
    if (SSL_set_fd(context, io_wrapper->sock_fd_) == 0)
      throw NetworkProcessException("Failed to set ssl fd");

    // ssl handshake is done, need to use new methods for the original wrappers;
    // We do not update the type in the reusable wrappers map because it is not
    // relevant.
    io_wrapper = new std::shared_ptr<NetworkIoWrapper>(
        reinterpret_cast<SslSocketIoWrapper *>(io_wrapper.get()));
  }

  // The wrapper already uses SSL methods.
  // Yuchen: "Post-connection verification?"
  auto *context = io_wrapper->conn_ssl_context_;
  ERR_clear_error();
  int ssl_accept_ret = SSL_accept(context);
  if (ssl_accept_ret > 0) return Transition::PROCEED;

  int err = SSL_get_error(context, ssl_accept_ret);
  switch (err) {
    case SSL_ERROR_WANT_READ:return Transition::NEED_READ;
    case SSL_ERROR_WANT_WRITE:return Transition::NEED_WRITE;
//      case SSL_ERROR_SSL:
//      case SSL_ERROR_ZERO_RETURN:
//      case SSL_ERROR_SYSCALL:
    default:LOG_ERROR("SSL Error, error code %d", err);
      return Transition::TERMINATE;
  }
}
} // namespace network
} // namespace peloton