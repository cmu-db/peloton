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
#include "wire/wire.h"
#include "common/exception.h"

#include <sys/un.h>
#include <string>

namespace peloton {
namespace wire {

template <typename B>
bool SocketManager<B>::RefillReadBuffer() {
  ssize_t bytes_read = 0;
  fd_set rset;
  bool done = false;

  // our buffer is to be emptied
  rbuf.Reset();

  // return explicitly
  while (!done) {
    while (bytes_read <= 0) {
      //  try to fill the available space in the buffer
      bytes_read = read(sock_fd, &rbuf.buf[rbuf.buf_ptr],
                        SOCKET_BUFFER_SIZE - rbuf.buf_size);

      // Read failed
      if (bytes_read < 0) {
        // Some other error occurred, close the socket, remove
        // the event and free the client structure.
        switch (errno) {
          case EINTR:
            LOG_DEBUG("Error Reading: EINTR");
            break;
          case EAGAIN:
            LOG_DEBUG("Error Reading: EAGAIN");
            break;
          case EBADF:
            LOG_DEBUG("Error Reading: EBADF");
            break;
          case EDESTADDRREQ:
            LOG_DEBUG("Error Reading: EDESTADDRREQ");
            break;
          case EDQUOT:
            LOG_DEBUG("Error Reading: EDQUOT");
            break;
          case EFAULT:
            LOG_DEBUG("Error Reading: EFAULT");
            break;
          case EFBIG:
            LOG_DEBUG("Error Reading: EFBIG");
            break;
          case EINVAL:
            LOG_DEBUG("Error Reading: EINVAL");
            break;
          case EIO:
            LOG_DEBUG("Error Reading: EIO");
            break;
          case ENOSPC:
            LOG_DEBUG("Error Reading: ENOSPC");
            break;
          case EPIPE:
            LOG_DEBUG("Error Reading: EPIPE");
            break;
          default:
            LOG_DEBUG("Error Reading: UNKNOWN");
        }
        if (errno == EINTR) {
          // interrupts are ok, try again
          bytes_read = 0;
          continue;

          // Read would have blocked if the scoket
          // was in blocking mode. Wait till it's readable
        } else if (errno == EAGAIN) {
          FD_ZERO(&rset);
          FD_SET(sock_fd, &rset);
          bytes_read = select(sock_fd + 1, &rset, NULL, NULL, NULL);
          if (bytes_read < 0) {
            LOG_INFO("bytes_read < 0 after select. Fatal");
            exit(EXIT_FAILURE);
          } else if (bytes_read == 0) {
            // timed out without writing any data
            LOG_INFO("Timeout without reading");
            exit(EXIT_FAILURE);
          }
          // else, socket is now readable, so loop back up and do the read()
          // again
          bytes_read = 0;
          continue;
        } else {
          // fatal errors
          return false;
        }
      } else if (bytes_read == 0) {
        // If the length of bytes returned by read is 0, this means
        // that the client disconnected
        disconnected = true;
        return false;
      } else {
        done = true;
      }
    }

    // read success, update buffer size
    rbuf.buf_size += bytes_read;

    // reset buffer ptr, to cover special case
    rbuf.buf_ptr = 0;
    return true;
  }
  return true;
}

template <typename B>
bool SocketManager<B>::FlushWriteBuffer() {
  fd_set wset;
  ssize_t written_bytes = 0;
  wbuf.buf_ptr = 0;
  // still outstanding bytes
  while ((int)wbuf.buf_size > 0) {
    written_bytes = 0;
    while (written_bytes <= 0) {
      written_bytes = write(sock_fd, &wbuf.buf[wbuf.buf_ptr], wbuf.buf_size);
      // Write failed
      if (written_bytes < 0) {
        switch (errno) {
          case EINTR:
            LOG_DEBUG("Error Writing: EINTR");
            break;
          case EAGAIN:
            LOG_DEBUG("Error Writing: EAGAIN");
            break;
          case EBADF:
            LOG_DEBUG("Error Writing: EBADF");
            break;
          case EDESTADDRREQ:
            LOG_DEBUG("Error Writing: EDESTADDRREQ");
            break;
          case EDQUOT:
            LOG_DEBUG("Error Writing: EDQUOT");
            break;
          case EFAULT:
            LOG_DEBUG("Error Writing: EFAULT");
            break;
          case EFBIG:
            LOG_DEBUG("Error Writing: EFBIG");
            break;
          case EINVAL:
            LOG_DEBUG("Error Writing: EINVAL");
            break;
          case EIO:
            LOG_DEBUG("Error Writing: EIO");
            break;
          case ENOSPC:
            LOG_DEBUG("Error Writing: ENOSPC");
            break;
          case EPIPE:
            LOG_DEBUG("Error Writing: EPIPE");
            break;
          default:
            LOG_DEBUG("Error Writing: UNKNOWN");
        }
        if (errno == EINTR) {
          // interrupts are ok, try again
          written_bytes = 0;
          continue;
          // Write would have blocked if the socket was
          // in blocking mode. Wait till it's readable
        } else if (errno == EAGAIN) {
          FD_ZERO(&wset);
          FD_SET(sock_fd, &wset);
          written_bytes = select(sock_fd + 1, NULL, &wset, NULL, NULL);
          if (written_bytes < 0) {
            LOG_INFO("written_bytes < 0 after select. Fatal");
            exit(EXIT_FAILURE);
          } else if (written_bytes == 0) {
            // timed out without writing any data
            LOG_INFO("Timeout without writing");
            exit(EXIT_FAILURE);
          }
          // else, socket is now readable, so loop back up and do the read()
          // again
          written_bytes = 0;
          continue;
        } else {
          // fatal errors
          return false;
        }
      }

      // weird edge case?
      if (written_bytes == 0 && wbuf.buf_size != 0) {
        LOG_INFO("Not all data is written");
        // fatal
        return false;
      }
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
void SocketManager<B>::PrintWriteBuffer() {
  LOG_TRACE("Write Buffer:");

  for (size_t i = 0; i < wbuf.buf_size; ++i) {
    LOG_TRACE("%u", wbuf.buf[i]);
  }
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
      if (!FlushWriteBuffer()) {
        return false;
      }
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
template class SocketManager<PktBuf>;

}  // End wire namespace
}  // End peloton namespace
