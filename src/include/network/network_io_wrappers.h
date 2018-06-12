//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// client_socket_wrapper.h
//
// Identification: src/include/network/client_socket_wrapper.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <openssl/ssl.h>
#include <memory>
#include <utility>
#include "network/marshal.h"
#include "common/exception.h"
#include "common/utility.h"

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
  // TODO(Tianyu): Change and document after we refactor protocol handler
  virtual Transition FillReadBuffer() = 0;
  virtual Transition FlushWriteBuffer() = 0;
  virtual Transition Close() = 0;

  inline int GetSocketFd() { return sock_fd_; }
  Transition WritePacket(OutputPacket *pkt);
  // TODO(Tianyu): Make these protected when protocol handler refactor is complete
  NetworkIoWrapper(int sock_fd,
                   std::shared_ptr<ReadBuffer> &rbuf,
                   std::shared_ptr<WriteBuffer> &wbuf)
      : sock_fd_(sock_fd),
        conn_ssl_context_(nullptr),
        rbuf_(std::move(rbuf)),
        wbuf_(std::move(wbuf)) {}
  // It is worth noting that because of the way we are reinterpret-casting between
  // derived types, it is necessary that they share the same members.
  int sock_fd_;
  std::shared_ptr<ReadBuffer> rbuf_;
  std::shared_ptr<WriteBuffer> wbuf_;
  SSL *conn_ssl_context_;
};

/**
 * A Network IoWrapper specialized for dealing with posix sockets.
 */
class PosixSocketIoWrapper : public NetworkIoWrapper {
 public:
  PosixSocketIoWrapper(int sock_fd,
                       std::shared_ptr<ReadBuffer> rbuf,
                       std::shared_ptr<WriteBuffer> wbuf);

  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer() override;
  inline Transition Close() override {
    peloton_close(sock_fd_);
    return Transition::NONE;
  }
};

/**
 * NetworkIoWrapper specialized for dealing with ssl sockets.
 */
class SslSocketIoWrapper : public NetworkIoWrapper {
 public:
  // An SslSocketIoWrapper is always derived from a PosixSocketIoWrapper,
  // as the handshake process happens over posix sockets. Use the method
  // in NetworkIoWrapperFactory to get an SslSocketWrapper.
  SslSocketIoWrapper() = delete;

  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer() override;
  Transition Close() override;
};
} // namespace network
} // namespace peloton