//
// Created by siddharth on 25/3/16.
//

#ifndef MEMCACHED_SOCKET_H
#define MEMCACHED_SOCKET_H

#include "../libpq/libpq-be.h"

#define MC_SOCK_BUFFER_SIZE_BYTES 8

// memcached db login credentials
extern char *memcached_dbname;
extern char *memcached_username;

/* wrapper over pgsocket used for memcached */
struct MemcachedSocket {
private:
  Port *port;
  size_t buf_ptr;
  size_t buf_size;
  std::string buffer;
public:
  inline MemcachedSocket(Port *port) :
      port(port), buf_ptr(0), buf_size(0),
      buffer(MC_SOCK_BUFFER_SIZE_BYTES, 0)
  {}

  inline void close_socket() {
    for (;;) {
      int status = (port->sock);
      if (status < 0) {
        // failed close
        if (errno == EINTR) {
          // interrupted, try closing again
          continue;
        }
      }
      return;
    }
  }

  bool refill_buffer();

  // store a line in buffer into new_line, return false
  // if read failed
  bool read_line(std::string &new_line);
};

#endif