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

#include "common/logger.h"

#define SOCKET_BUFFER_SIZE 8192
#define MAX_CONNECTIONS 64
#define DEFAULT_PORT 5432

namespace peloton {
namespace wire {

class PacketManager;

typedef unsigned char uchar;

// use array as memory for the socket buffers can be fixed
typedef std::array<uchar, SOCKET_BUFFER_SIZE> SockBuf;

struct Server {
  int port;
  int server_fd;
  int max_connections;

  inline Server(int port, int max_conn)
      : port(port), server_fd(0), max_connections(max_conn) {}
};

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
  inline SocketManager(int sock_fd) : sock_fd(sock_fd) {}

  // Reads a packet of length "bytes" from the head of the buffer
  bool ReadBytes(B &pkt_buf, size_t bytes);

  // Writes a packet into the write buffer
  bool BufferWriteBytes(B &pkt_buf, size_t len, uchar type);

  // Used to invoke a write into the Socket, once the write buffer is ready
  bool FlushWriteBuffer();

  void CloseSocket();
};

extern void StartServer(Server *server);

// Thread function created per client
template <typename P, typename B>
void ClientHandler(std::unique_ptr<int> clientfd);

// Server's "accept loop"
template <typename P, typename B>
void HandleConnections(Server *server);

/*
 * Functions defined here for template visibility
 *
 */

/*
 * handle_connections - Server's accept loop. Takes the protocol's PacketManager
 * (P) and STL container type for the protocol's buffer (B)
 */

template <typename P, typename B>
void HandleConnections(Server *server) {
  int connfd, clilen;
  struct sockaddr_in cli_addr;
  clilen = sizeof(cli_addr);

  for (;;) {
    // block and wait for incoming connection
    connfd = accept(server->server_fd, (struct sockaddr *)&cli_addr,
                    (socklen_t *)&clilen);
    if (connfd < 0) {
      LOG_ERROR("Server error: Connection not established");
      exit(EXIT_FAILURE);
    }

    std::unique_ptr<int> clientfd(new int(connfd));

    std::cout << "LAUNCHING NEW THREAD" << std::endl;
    std::thread client_thread(ClientHandler<P, B>, std::move(clientfd));
    client_thread.detach();
  }
}

/*
 * client_handler - Thread function to handle a client.
 * 		Takes the protocol's PacketManager (P) and STL container
 * 		type for the protocol's buffer (B)
 */
template <typename P, typename B>
void ClientHandler(std::unique_ptr<int> clientfd) {
  int fd = *clientfd;
  LOG_INFO("Client fd: %d", fd);
  SocketManager<B> sm(fd);
  P p(&sm);
  p.ManagePackets();
}
}
}
