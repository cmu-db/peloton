//
// Created by Siddharth Santurkar on 31/3/16.
//

#ifndef PELOTON_SOCKET_BASE_H
#define PELOTON_SOCKET_BASE_H

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
#include "logger.h"
#include "globals.h"

#define SOCKET_BUFFER_SIZE 8192
#define MAX_CONNECTIONS 64

namespace peloton {
namespace wire {

class PacketManager;

typedef unsigned char uchar;

typedef std::array<uchar, SOCKET_BUFFER_SIZE> SockBuf;


struct Server {
  int port;
  int server_fd;
  int max_connections;

  inline Server(int port, int max_conn)
      : port(port), max_connections(max_conn) {}
};

struct Buffer {
  size_t buf_ptr;
  size_t buf_size;
  SockBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0) {}

  inline void reset() {
    buf_ptr = 0;
    buf_size = 0;
  }

  inline size_t get_max_size() { return SOCKET_BUFFER_SIZE; }
};
/*
 * SocektManager - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
template <typename B>
class SocketManager {
  int sock_fd;
  Buffer rbuf;
  Buffer wbuf;

 private:
  bool refill_read_buffer();

 public:
  inline SocketManager(int sock_fd) : sock_fd(sock_fd) {}

  bool read_bytes(B &pkt_buf, size_t bytes);

  bool buffer_write_bytes(B &pkt_buf, size_t len, uchar type);

  bool flush_write_buffer();

  void close_socket();
};

extern void start_server(Server *server);

template <typename P, typename B>
void client_handler(ThreadGlobals& globals, std::unique_ptr<int> clientfd);

template <typename P, typename B>
void handle_connections(Server *server);

/*
 * Functions defined here for template visibility
 *
 */

/*
 * handle_connections - Server's accept loop. Takes the protocol's PacketManager
 * (P)
 * 		and STL container type for the protocol's buffer (B)
 */

template <typename P, typename B>
void handle_connections(Server *server) {
  int connfd, clilen;
  struct sockaddr_in cli_addr;
  clilen = sizeof(cli_addr);
  ThreadGlobals globals;

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
    std::thread client_thread(client_handler<P, B>, std::ref(globals), std::move(clientfd));
    client_thread.detach();
  }
}

/*
 * client_handler - Thread function to handle a client.
 * 		Takes the protocol's PacketManager (P) and STL container
 * 		type for the protocol's buffer (B)
 */
template <typename P, typename B>
void client_handler(ThreadGlobals& globals, std::unique_ptr<int> clientfd) {
  int fd = *clientfd;
  LOG_INFO("Client fd: %d", fd);
  SocketManager<B> sm(fd);
  P p(&sm);
  p.manage_packets(globals);
}
}
}
#endif  // PELOTON_SOCKET_BASE_H
