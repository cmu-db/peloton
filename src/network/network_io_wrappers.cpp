//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_io_wrappers.cpp
//
// Identification: src/network/network_io_wrappers.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/network_io_wrappers.h"
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <sys/file.h>
#include "network/peloton_server.h"

namespace peloton {
namespace network {
Transition NetworkIoWrapper::FlushAllWrites() {
  for (; out_->FlushHead() != nullptr; out_->MarkHeadFlushed()) {
    auto result = FlushWriteBuffer(*out_->FlushHead());
    if (result != Transition::PROCEED) return result;
  }
  out_->Reset();
  return Transition::PROCEED;
}

PosixSocketIoWrapper::PosixSocketIoWrapper(int sock_fd,
                                           std::shared_ptr<ReadBuffer> in,
                                           std::shared_ptr<WriteQueue> out)
    : NetworkIoWrapper(sock_fd, std::move(in), std::move(out)) {

  // Set Non Blocking
  auto flags = fcntl(sock_fd_, F_GETFL);
  flags |= O_NONBLOCK;
  if (fcntl(sock_fd_, F_SETFL, flags) < 0) {
    LOG_ERROR("Failed to set non-blocking socket");
  }
  // Set TCP No Delay
  int one = 1;
  setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

Transition PosixSocketIoWrapper::FillReadBuffer() {
  if (!in_->HasMore()) in_->Reset();
  if (in_->HasMore() && in_->Full()) in_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  // Normal mode
  while (!in_->Full()) {
    auto bytes_read = in_->FillBufferFrom(sock_fd_);
    if (bytes_read > 0)
      result = Transition::PROCEED;
    else if (bytes_read == 0)
      return Transition::TERMINATE;
    else
      switch (errno) {
        case EAGAIN:
          // Equal to EWOULDBLOCK
          return result;
        case EINTR:continue;
        default:LOG_ERROR("Error writing: %s", strerror(errno));
          throw NetworkProcessException("Error when filling read buffer " +
              std::to_string(errno));
      }
  }
  return result;
}

Transition PosixSocketIoWrapper::FlushWriteBuffer(WriteBuffer &wbuf) {
  while (wbuf.HasMore()) {
    auto bytes_written = wbuf.WriteOutTo(sock_fd_);
    if (bytes_written < 0)
      switch (errno) {
        case EINTR:continue;
        case EAGAIN:return Transition::NEED_WRITE;
        default:LOG_ERROR("Error writing: %s", strerror(errno));
          throw NetworkProcessException("Fatal error during write");
      }
  }
  wbuf.Reset();
  return Transition::PROCEED;
}

Transition SslSocketIoWrapper::FillReadBuffer() {
  if (!in_->HasMore()) in_->Reset();
  if (in_->HasMore() && in_->Full()) in_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  while (!in_->Full()) {
    auto ret = in_->FillBufferFrom(conn_ssl_context_);
    switch (ret) {
      case SSL_ERROR_NONE:result = Transition::PROCEED;
        break;
      case SSL_ERROR_ZERO_RETURN: return Transition::TERMINATE;
        // The SSL packet is partially loaded to the SSL buffer only,
        // More data is required in order to decode the wh`ole packet.
      case SSL_ERROR_WANT_READ: return result;
      case SSL_ERROR_WANT_WRITE: return Transition::NEED_WRITE;
      case SSL_ERROR_SYSCALL:
        if (errno == EINTR) {
          LOG_INFO("Error SSL Reading: EINTR");
          break;
        }
        // Intentional fallthrough
      default:
        throw NetworkProcessException("SSL read error: " + std::to_string(ret));
    }
  }
  return result;
}

Transition SslSocketIoWrapper::FlushWriteBuffer(WriteBuffer &wbuf) {
  while (wbuf.HasMore()) {
    auto ret = wbuf.WriteOutTo(conn_ssl_context_);
    switch (ret) {
      case SSL_ERROR_NONE: break;
      case SSL_ERROR_WANT_WRITE: return Transition::NEED_WRITE;
      case SSL_ERROR_WANT_READ: return Transition::NEED_READ;
      case SSL_ERROR_SYSCALL:
        // If interrupted, try again.
        if (errno == EINTR) {
          LOG_TRACE("Flush write buffer, eintr");
          break;
        }
        // Intentional Fallthrough
      default:LOG_ERROR("SSL write error: %d, error code: %lu",
                        ret,
                        ERR_get_error());
        throw NetworkProcessException("SSL write error");
    }
  }
  wbuf.Reset();
  return Transition::PROCEED;
}

Transition SslSocketIoWrapper::Close() {
  ERR_clear_error();
  int ret = SSL_shutdown(conn_ssl_context_);
  if (ret != 0) {
    int err = SSL_get_error(conn_ssl_context_, ret);
    switch (err) {
      // More work to do before shutdown
      case SSL_ERROR_WANT_READ: return Transition::NEED_READ;
      case SSL_ERROR_WANT_WRITE: return Transition::NEED_WRITE;
      default: LOG_ERROR("Error shutting down ssl session, err: %d", err);
    }
  }
  // SSL context is explicitly deallocated here because socket wrapper
  // objects are saved reused for memory efficiency and the reuse might
  // not happen immediately, and thus freeing it on reuse time can make this
  // live on arbitrarily long.
  SSL_free(conn_ssl_context_);
  conn_ssl_context_ = nullptr;
  peloton_close(sock_fd_);
  return Transition::PROCEED;
}

}  // namespace network
}  // namespace peloton
