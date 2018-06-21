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
Transition NetworkIoWrapper::WritePacket(OutputPacket *pkt) {
  // Write Packet Header
  if (!pkt->skip_header_write) {
    if (!wbuf_->HasSpaceFor(1 + sizeof(int32_t))) {
      auto result = FlushWriteBuffer();
      if (FlushWriteBuffer() != Transition::PROCEED)
        // Unable to flush buffer, socket presumably not ready for write
        return result;
    }

    wbuf_->Append(static_cast<unsigned char>(pkt->msg_type));
    if (!pkt->single_type_pkt)
      // Need to convert bytes to network order
      wbuf_->Append(htonl(pkt->len + sizeof(int32_t)));
    pkt->skip_header_write = true;
  }

  // Write Packet Content
  for (size_t len = pkt->len; len != 0;) {
    if (wbuf_->HasSpaceFor(len)) {
      wbuf_->Append(std::begin(pkt->buf) + pkt->write_ptr, len);
      break;
    } else {
      auto write_size = wbuf_->RemainingCapacity();
      wbuf_->Append(std::begin(pkt->buf) + pkt->write_ptr, write_size);
      len -= write_size;
      pkt->write_ptr += write_size;
      auto result = FlushWriteBuffer();
      if (FlushWriteBuffer() != Transition::PROCEED)
        // Unable to flush buffer, socket presumably not ready for write
        return result;
    }
  }
  return Transition::PROCEED;
}

PosixSocketIoWrapper::PosixSocketIoWrapper(int sock_fd,
                                           std::shared_ptr<ReadBuffer> rbuf,
                                           std::shared_ptr<WriteBuffer> wbuf)
    : NetworkIoWrapper(sock_fd, rbuf, wbuf) {
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
  if (!rbuf_->HasMore()) rbuf_->Reset();
  if (rbuf_->HasMore() && rbuf_->Full()) rbuf_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  // Normal mode
  while (!rbuf_->Full()) {
    auto bytes_read = rbuf_->FillBufferFrom(sock_fd_);
    if (bytes_read > 0)
      result = Transition::PROCEED;
    else if (bytes_read == 0)
      return Transition::TERMINATE;
    else
      switch (errno) {
        case EAGAIN:
          // Equal to EWOULDBLOCK
          return result;
        case EINTR:
          continue;
        default:
          LOG_ERROR("Error writing: %s", strerror(errno));
          throw NetworkProcessException("Error when filling read buffer " +
                                        std::to_string(errno));
      }
  }
  return result;
}

Transition PosixSocketIoWrapper::FlushWriteBuffer() {
  while (wbuf_->HasMore()) {
    auto bytes_written = wbuf_->WriteOutTo(sock_fd_);
    if (bytes_written < 0) switch (errno) {
        case EINTR:
          continue;
        case EAGAIN:
          return Transition::NEED_WRITE;
        default:
          LOG_ERROR("Error writing: %s", strerror(errno));
          throw NetworkProcessException("Fatal error during write");
      }
  }
  wbuf_->Reset();
  return Transition::PROCEED;
}

Transition SslSocketIoWrapper::FillReadBuffer() {
  if (!rbuf_->HasMore()) rbuf_->Reset();
  if (rbuf_->HasMore() && rbuf_->Full()) rbuf_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  while (!rbuf_->Full()) {
    auto ret = rbuf_->FillBufferFrom(conn_ssl_context_);
    switch (ret) {
      case SSL_ERROR_NONE:
        result = Transition::PROCEED;
        break;
      case SSL_ERROR_ZERO_RETURN:
        return Transition::TERMINATE;
        // The SSL packet is partially loaded to the SSL buffer only,
        // More data is required in order to decode the wh`ole packet.
      case SSL_ERROR_WANT_READ:
        return result;
      case SSL_ERROR_WANT_WRITE:
        return Transition::NEED_WRITE;
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

Transition SslSocketIoWrapper::FlushWriteBuffer() {
  while (wbuf_->HasMore()) {
    auto ret = wbuf_->WriteOutTo(conn_ssl_context_);
    switch (ret) {
      case SSL_ERROR_NONE:
        break;
      case SSL_ERROR_WANT_WRITE:
        return Transition::NEED_WRITE;
      case SSL_ERROR_WANT_READ:
        return Transition::NEED_READ;
      case SSL_ERROR_SYSCALL:
        // If interrupted, try again.
        if (errno == EINTR) {
          LOG_TRACE("Flush write buffer, eintr");
          break;
        }
        // Intentional Fallthrough
      default:
        LOG_ERROR("SSL write error: %d, error code: %lu", ret, ERR_get_error());
        throw NetworkProcessException("SSL write error");
    }
  }
  wbuf_->Reset();
  return Transition::PROCEED;
}

Transition SslSocketIoWrapper::Close() {
  ERR_clear_error();
  int ret = SSL_shutdown(conn_ssl_context_);
  if (ret != 0) {
    int err = SSL_get_error(conn_ssl_context_, ret);
    switch (err) {
      case SSL_ERROR_WANT_WRITE:
        return Transition::NEED_WRITE;
      case SSL_ERROR_WANT_READ:
        // More work to do before shutdown
        return Transition::NEED_READ;
      default:
        LOG_ERROR("Error shutting down ssl session, err: %d", err);
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
