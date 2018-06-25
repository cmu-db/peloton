//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_io_wrappers.h
//
// Identification: src/include/network/network_io_wrappers.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <openssl/ssl.h>
#include <memory>
#include <utility>
#include "common/exception.h"
#include "common/utility.h"
#include "network/marshal.h"

namespace peloton {
namespace network {

/**
 * A network io wrapper provides an interface for interacting with a client
 * connection.
 *
 * Underneath the hood the wrapper buffers read and write, and can support posix
 * and ssl reads and writes to the socket, depending on the concrete type at
 * runtime.
 *
 * Because the buffers are large and expensive to allocate on fly, they are
 * reused. Consequently, initialization of this class is handled by a factory
 * class. @see NetworkIoWrapperFactory
 */
class NetworkIoWrapper {
  friend class NetworkIoWrapperFactory;

 public:
  virtual bool SslAble() const = 0;
  // TODO(Tianyu): Change and document after we refactor protocol handler
  virtual Transition FillReadBuffer() = 0;
  virtual Transition FlushWriteBuffer() = 0;
  virtual Transition Close() = 0;

  inline int GetSocketFd() { return sock_fd_; }
  Transition WritePacket(OutputPacket *pkt);
  // TODO(Tianyu): Make these protected when protocol handler refactor is
  // complete
  NetworkIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> &rbuf,
                   std::shared_ptr<WriteBuffer> &wbuf)
      : sock_fd_(sock_fd),
        rbuf_(std::move(rbuf)),
        wbuf_(std::move(wbuf)) {
    rbuf_->Reset();
    wbuf_->Reset();
  }

  DISALLOW_COPY(NetworkIoWrapper)

  NetworkIoWrapper(NetworkIoWrapper &&other) = default;

  int sock_fd_;
  std::shared_ptr<ReadBuffer> rbuf_;
  std::shared_ptr<WriteBuffer> wbuf_;
};

/**
 * A Network IoWrapper specialized for dealing with posix sockets.
 */
class PosixSocketIoWrapper : public NetworkIoWrapper {
 public:
  PosixSocketIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> rbuf,
                       std::shared_ptr<WriteBuffer> wbuf);


  inline bool SslAble() const override { return false; }
  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer() override;
  inline Transition Close() override {
    peloton_close(sock_fd_);
    return Transition::PROCEED;
  }
};

/**
 * NetworkIoWrapper specialized for dealing with ssl sockets.
 */
class SslSocketIoWrapper : public NetworkIoWrapper {
 public:
  // Realistically, an SslSocketIoWrapper is always derived from a
  // PosixSocketIoWrapper, as the handshake process happens over posix sockets.
  SslSocketIoWrapper(NetworkIoWrapper &&other, SSL *ssl)
    : NetworkIoWrapper(std::move(other)), conn_ssl_context_(ssl) {}

  inline bool SslAble() const override { return true; }
  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer() override;
  Transition Close() override;

 private:
  friend class NetworkIoWrapperFactory;
  SSL *conn_ssl_context_;
};
}  // namespace network
}  // namespace peloton
