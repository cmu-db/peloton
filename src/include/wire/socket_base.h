//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// socket_base.h
//
// Identification: src/include/wire/socket_base.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <vector>
#include <thread>
#include <cstring>
#include <mutex>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <iostream>
#include <event2/event.h>
#include <chrono>

#include "common/logger.h"
#include "common/config.h"
#include "common/types.h"

#define SOCKET_BUFFER_SIZE 8192
#define MAX_CONNECTIONS 64
#define DEFAULT_PORT 5432

using namespace std::chrono;

namespace peloton {
namespace wire {

class PacketManager;
class Server;
// use array as memory for the socket buffers can be fixed
typedef std::array<uchar, SOCKET_BUFFER_SIZE> SockBuf;

// Buffers used to batch meesages at the socket
struct Buffer {
  size_t buf_ptr;   // buffer cursor
  size_t buf_size;  // buffer size
  SockBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0) {}

  inline void Reset() {
    buf_ptr = 0;
    buf_size = 0;
  }

  inline size_t GetMaxSize() { return SOCKET_BUFFER_SIZE; }
};
/*
 * SocektManager - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
template <typename B>
class SocketManager {
  int sock_fd;  // file descriptor
  Buffer rbuf;  // socket's read buffer
  Buffer wbuf;  // socket's write buffer

 private:
  /* refill_read_buffer - Used to repopulate read buffer with a fresh
   * batch of data from the socket
   */
  bool RefillReadBuffer();

 public:
  unsigned int id;
  bool disconnected;
  std::unique_ptr<PacketManager> socket_pkt_manager;
  SocketManager<B>* self;

  inline SocketManager(int sock_fd, unsigned int assigned_id) : sock_fd(sock_fd),
		  id(assigned_id), disconnected(false), self(NULL) { }

  int GetSocketFD() { return sock_fd; }

  // Reads a packet of length "bytes" from the head of the buffer
  bool ReadBytes(B &pkt_buf, size_t bytes);

  // Writes a packet into the write buffer
  bool BufferWriteBytes(B &pkt_buf, size_t len, uchar type);

  void PrintWriteBuffer();

  // Used to invoke a write into the Socket, once the write buffer is ready
  bool FlushWriteBuffer();

  void CloseSocket();
};

// Thread function created per client
template <typename P, typename B>
void ClientHandler(std::unique_ptr<int> clientfd);

// Server's "accept loop"
template <typename P, typename B>
void HandleConnections(Server *server);

}
}
