//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// socket_base.cpp
//
// Identification: src/wire/socket_base.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "wire/socket_base.h"

namespace peloton {
namespace wire {

void start_server(Server *server) {
  struct sockaddr_in serv_addr;
  int yes = 1;

  server->server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server->server_fd < 0) {
    LOG_ERROR("Server error: while opening socket");
    exit(EXIT_FAILURE);
  }

  if (setsockopt(server->server_fd, SOL_SOCKET, SO_REUSEADDR, &yes,
                 sizeof(yes)) == -1) {
    LOG_ERROR("Setsockopt error: can't config reuse addr");
    exit(EXIT_FAILURE);
  }

  memset(&serv_addr, 0, sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(server->port);

  if (bind(server->server_fd, (struct sockaddr *)&serv_addr,
           sizeof(serv_addr)) < 0) {
    LOG_ERROR("Server error: while binding");
    exit(EXIT_FAILURE);
  }

  listen(server->server_fd, server->max_connections);
}

template <typename B>
bool SocketManager<B>::refill_read_buffer() {
  ssize_t bytes_read;

  // our buffer is to be emptied
  rbuf.reset();

  // return explicitly
  for (;;) {
    //  try to fill the available space in the buffer
    bytes_read = read(sock_fd, &rbuf.buf[rbuf.buf_ptr],
                      SOCKET_BUFFER_SIZE - rbuf.buf_size);
    LOG_INFO("Bytes Read: %lu", bytes_read);
    if (bytes_read < 0) {
      if (errno == EINTR) {
        // interrupts are OK
        continue;
      }

      // otherwise, report error
      LOG_ERROR("Socket error: could not receive data from client");
      return false;
    }

    if (bytes_read == 0) {
      // EOF, return
      return false;
    }

    // read success, update buffer size
    rbuf.buf_size += bytes_read;

    // reset buffer ptr, to cover special case
    rbuf.buf_ptr = 0;
    return true;
  }
}

template <typename B>
bool SocketManager<B>::flush_write_buffer() {
  ssize_t written_bytes = 0;
  wbuf.buf_ptr = 0;
  // still outstanding bytes
  while (wbuf.buf_size - written_bytes > 0) {
    written_bytes = write(sock_fd, &wbuf.buf[wbuf.buf_ptr], wbuf.buf_size);
    if (written_bytes < 0) {
      if (errno == EINTR) {
        // interrupts are ok, try again
        continue;
      } else {
        // fatal errors
        return false;
      }
    }

    // weird edge case?
    if (written_bytes == 0 && wbuf.buf_size != 0) {
      // fatal
      return false;
    }

    // update bookkeping
    wbuf.buf_ptr += written_bytes;
    wbuf.buf_size -= written_bytes;
  }

  // buffer is empty
  wbuf.reset();

  // we are ok
  return true;
}

/*
 * read - Tries to read "bytes" bytes into packet's buffer. Returns true on
 * success.
 * 		false on failure. B can be any STL container.
 */
template <typename B>
bool SocketManager<B>::read_bytes(B &pkt_buf, size_t bytes) {
  size_t window, pkt_buf_idx = 0;
  // while data still needs to be read
  while (bytes) {
    // how much data is available
    window = rbuf.buf_size - rbuf.buf_ptr;
    if (bytes <= window) {
      pkt_buf.insert(std::end(pkt_buf), std::begin(rbuf.buf) + rbuf.buf_ptr,
                     std::begin(rbuf.buf) + rbuf.buf_ptr + bytes);

      // move the pointer
      rbuf.buf_ptr += bytes;

      // move pkt_buf_idx as well
      pkt_buf_idx += bytes;

      return true;
    } else {
      // read what is available for non-trivial window
      if (window > 0) {
        pkt_buf.insert(std::end(pkt_buf), std::begin(rbuf.buf) + rbuf.buf_ptr,
                       std::begin(rbuf.buf) + rbuf.buf_size);

        // update bytes leftover
        bytes -= window;

        // update pkt_buf_idx
        pkt_buf_idx += window;
      }

      // refill buffer, reset buf ptr here
      if (!refill_read_buffer()) {
        // nothing more to read, end
        return false;
      }
    }
  }

  return true;
}

template <typename B>
bool SocketManager<B>::buffer_write_bytes(B &pkt_buf, size_t len, uchar type) {
  size_t window, pkt_buf_ptr = 0;
  int len_nb;  // length in network byte order

  // check if we don't have enough space in the buffer
  if (wbuf.get_max_size() - wbuf.buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    flush_write_buffer();
  }

  // assuming wbuf is now large enough to
  // fit type and size fields in one go
  if (type != 0) {
    // type shouldn't be ignored
    wbuf.buf[wbuf.buf_ptr++] = type;
  }

  // make len include its field size as well
  len_nb = htonl(len + sizeof(int32_t));

  // append the bytes of this integer in network-byte order
  std::copy(reinterpret_cast<uchar *>(&len_nb),
            reinterpret_cast<uchar *>(&len_nb) + 4,
            std::begin(wbuf.buf) + wbuf.buf_ptr);

  // move the write buffer pointer and update size of the socket buffer
  wbuf.buf_ptr += sizeof(int32_t);
  wbuf.buf_size = wbuf.buf_ptr;

  // fill the contents
  while (len) {
    window = wbuf.get_max_size() - wbuf.buf_ptr;
    if (len <= window) {
      // contents fit in the window, range copy "len" bytes
      std::copy(std::begin(pkt_buf) + pkt_buf_ptr,
                std::begin(pkt_buf) + pkt_buf_ptr + len,
                std::begin(wbuf.buf) + wbuf.buf_ptr);

      // Move the cursor and update size of socket buffer
      wbuf.buf_ptr += len;
      wbuf.buf_size = wbuf.buf_ptr;
      return true;
    } else {
      /* contents longer than socket buffer size, fill up the socket buffer
       *  with "window" bytes
       */
      std::copy(std::begin(pkt_buf) + pkt_buf_ptr,
                std::begin(pkt_buf) + pkt_buf_ptr + window,
                std::begin(wbuf.buf) + wbuf.buf_ptr);

      // move the packet's cursor
      pkt_buf_ptr += window;
      len -= window;

      wbuf.buf_size = wbuf.get_max_size();
      // write failure
      if (!flush_write_buffer()) return false;
    }
  }
  return true;
}

template <typename B>
void SocketManager<B>::close_socket() {
  for (;;) {
    int status = close(sock_fd);
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

// explicit template instantiation for read_bytes
template class SocketManager<std::vector<uchar>>;
}
}
