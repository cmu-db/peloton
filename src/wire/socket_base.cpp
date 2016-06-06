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

#include <sys/un.h>

#include <string>

namespace peloton {
namespace wire {

void StartServer(Server *server) {
  struct sockaddr_un serv_addr;
  int yes = 1;
  int ret;
  int len;

  std::string SOCKET_PATH = "/tmp/.s.PGSQL." + std::to_string(server->port);
  LOG_INFO("Socket path : %s", SOCKET_PATH.c_str());

  server->server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server->server_fd < 0) {
    LOG_ERROR("Server error: while opening socket");
    exit(EXIT_FAILURE);
  }

  ret = setsockopt(server->server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  if (ret == -1) {
    LOG_ERROR("Setsockopt error: can't config reuse addr");
    exit(EXIT_FAILURE);
  }

  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sun_family = AF_UNIX;
  strncpy(serv_addr.sun_path, SOCKET_PATH.c_str(), sizeof(serv_addr.sun_path) - 1);
  unlink(serv_addr.sun_path);

  len = strlen(serv_addr.sun_path) + sizeof(serv_addr.sun_family);
  ret = bind(server->server_fd, (struct sockaddr *)&serv_addr, len);
  if (ret == -1) {
    LOG_ERROR("Server error: while binding");
    perror("bind");
    exit(EXIT_FAILURE);
  }

  ret = listen(server->server_fd, server->max_connections);
  if (ret == -1) {
    LOG_ERROR("Server error: while listening");
    perror("listen");
    exit(EXIT_FAILURE);
  }

}

template <typename B>
bool SocketManager<B>::RefillReadBuffer() {
  ssize_t bytes_read;

  // our buffer is to be emptied
  rbuf.Reset();

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
bool SocketManager<B>::FlushWriteBuffer() {
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
  wbuf.Reset();

  // we are ok
  return true;
}

/*
 * read - Tries to read "bytes" bytes into packet's buffer. Returns true on
 * success.
 * 		false on failure. B can be any STL container.
 */
template <typename B>
bool SocketManager<B>::ReadBytes(B &pkt_buf, size_t bytes) {
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
      if (!RefillReadBuffer()) {
        // nothing more to read, end
        return false;
      }
    }
  }

  return true;
}

template <typename B>
bool SocketManager<B>::BufferWriteBytes(B &pkt_buf, size_t len, uchar type) {
  size_t window, pkt_buf_ptr = 0;
  int len_nb;  // length in network byte order

  // check if we don't have enough space in the buffer
  if (wbuf.GetMaxSize() - wbuf.buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    FlushWriteBuffer();
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
    window = wbuf.GetMaxSize() - wbuf.buf_ptr;
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

      wbuf.buf_size = wbuf.GetMaxSize();
      // write failure
      if (!FlushWriteBuffer()) return false;
    }
  }
  return true;
}

template <typename B>
void SocketManager<B>::CloseSocket() {
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

}  // End wire namespace
}  // End peloton namespace

