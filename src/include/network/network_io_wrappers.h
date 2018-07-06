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
#include "network/network_types.h"
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
 public:
  virtual bool SslAble() const = 0;
  // TODO(Tianyu): Change and document after we refactor protocol handler
  virtual Transition FillReadBuffer() = 0;
  virtual Transition FlushWriteBuffer(WriteBuffer &wbuf) = 0;
  virtual Transition Close() = 0;

  inline int GetSocketFd() { return sock_fd_; }
  inline std::shared_ptr<ReadBuffer> GetReadBuffer() { return in_; }
  inline std::shared_ptr<WriteQueue> GetWriteQueue() { return out_; }
  Transition FlushAllWrites();
  inline bool ShouldFlush() { return out_->ShouldFlush(); }
  // TODO(Tianyu): Make these protected when protocol handler refactor is
  // complete
  NetworkIoWrapper(int sock_fd,
                   std::shared_ptr<ReadBuffer> in,
                   std::shared_ptr<WriteQueue> out)
      : sock_fd_(sock_fd),
        in_(std::move(in)),
        out_(std::move(out)) {
    in_->Reset();
    out_->Reset();
  }

  DISALLOW_COPY(NetworkIoWrapper);

  NetworkIoWrapper(NetworkIoWrapper &&other) noexcept
      : NetworkIoWrapper(other.sock_fd_,
                         std::move(other.in_),
                         std::move(other.out_)) {}

  int sock_fd_;
  std::shared_ptr<ReadBuffer> in_;
  std::shared_ptr<WriteQueue> out_;
};

/**
 * A Network IoWrapper specialized for dealing with posix sockets.
 */
class PosixSocketIoWrapper : public NetworkIoWrapper {
 public:
  explicit PosixSocketIoWrapper(int sock_fd,
                                std::shared_ptr<ReadBuffer> in =
                                    std::make_shared<ReadBuffer>(),
                                std::shared_ptr<WriteQueue> out =
                                    std::make_shared<WriteQueue>());

  explicit PosixSocketIoWrapper(NetworkIoWrapper &&other)
      : PosixSocketIoWrapper(other.sock_fd_,
                             std::move(other.in_),
                             std::move(other.out_)) {}

  DISALLOW_COPY_AND_MOVE(PosixSocketIoWrapper);

  inline bool SslAble() const override { return false; }
  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer(WriteBuffer &wbuf) override;
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

  DISALLOW_COPY_AND_MOVE(SslSocketIoWrapper);

  inline bool SslAble() const override { return true; }
  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer(WriteBuffer &wbuf) override;
  Transition Close() override;

 private:
  friend class ConnectionHandle;
  SSL *conn_ssl_context_;
};
}  // namespace network
}  // namespace peloton
