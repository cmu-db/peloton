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

#include <unistd.h>
#include "wire/libevent_server.h"

namespace peloton {
namespace wire {

/**
 * Public Functions
 */

bool LibeventSocket::WritePackets(const bool &force_flush) {

  // iterate through all the packets
  for (; next_response_ < responses.size(); next_response_++) {
    auto pkt = responses[next_response_].get();
    // write is not ready during write. transit to CONN_WRITE
    if (BufferWriteBytesHeader(pkt) == false ||
        BufferWriteBytesContent(pkt) == false) {
      return false;
    }
  }

  // Done writing all packets. clear packets
  responses.clear();
  next_response_ = 0;

  if (force_flush) {
    return FlushWriteBuffer();
  }
  return true;
}

/**
 * Private Functions
 */
bool LibeventSocket::RefillReadBuffer() {
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
        is_disconnected = true;
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

bool LibeventSocket::FlushWriteBuffer() {
  ssize_t written_bytes = 0;
  // while we still have outstanding bytes to write
  while ((int)wbuf.buf_size > 0) {
    written_bytes = 0;
    while (written_bytes <= 0) {
      written_bytes =
          write(sock_fd, &wbuf.buf[wbuf.buf_flush_ptr], wbuf.buf_size);
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
          // We should go to CONN_WRITE state
          return false;
        } else {
          // fatal errors
          throw ConnectionException("Fatal error during write");
        }
      }

      // weird edge case?
      if (written_bytes == 0 && wbuf.buf_size != 0) {
        LOG_INFO("Not all data is written");
        // fatal error
        throw ConnectionException("Not all data is written");
      }
    }

    // update book keeping
    wbuf.buf_flush_ptr += written_bytes;
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
bool LibeventSocket::ReadBytes(PktBuf &pkt_buf, size_t bytes) {
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

void LibeventSocket::PrintWriteBuffer() {
  LOG_TRACE("Write Buffer:");

  for (size_t i = 0; i < wbuf.buf_size; ++i) {
    LOG_TRACE("%u", wbuf.buf[i]);
  }
}

// Writes a packet's header (type, size) into the write buffer.
// Return false when the socket is not ready for write
bool LibeventSocket::BufferWriteBytesHeader(Packet *pkt) {
  // If we should not write
  if (pkt->skip_header_write) {
    return true;
  }

  size_t len = pkt->len;
  uchar type = pkt->msg_type;
  int len_nb;  // length in network byte order

  // check if we have enough space in the buffer
  if (wbuf.GetMaxSize() - wbuf.buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    if (FlushWriteBuffer() == false) {
      // Socket is not ready for write
      return false;
    }
  }

  // assuming wbuf is now large enough to fit type and size fields in one go
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

  // Header is written to socket buf. No need to write it in the future
  pkt->skip_header_write = true;
  return true;
}

// Writes a packet's content into the write buffer
// Return false when the socket is not ready for write
bool LibeventSocket::BufferWriteBytesContent(Packet *pkt) {
  // the packet content to write
  PktBuf &pkt_buf = pkt->buf;
  // the length of remaining content to write
  size_t len = pkt->len;
  // window is the size of remaining space in socket's wbuf
  size_t window = 0;

  // fill the contents
  while (len) {
    // calculate the remaining space in wbuf
    window = wbuf.GetMaxSize() - wbuf.buf_ptr;
    if (len <= window) {
      // contents fit in the window, range copy "len" bytes
      std::copy(std::begin(pkt_buf) + pkt->write_ptr,
                std::begin(pkt_buf) + pkt->write_ptr + len,
                std::begin(wbuf.buf) + wbuf.buf_ptr);

      // Move the cursor and update size of socket buffer
      wbuf.buf_ptr += len;
      wbuf.buf_size = wbuf.buf_ptr;
      LOG_DEBUG("Content fit in window. Write content successful");
      return true;
    } else {
      // contents longer than socket buffer size, fill up the socket buffer
      // with "window" bytes

      std::copy(std::begin(pkt_buf) + pkt->write_ptr,
                std::begin(pkt_buf) + pkt->write_ptr + window,
                std::begin(wbuf.buf) + wbuf.buf_ptr);

      // move the packet's cursor
      pkt->write_ptr += window;
      len -= window;
      // Now the wbuf is full
      wbuf.buf_size = wbuf.GetMaxSize();

      LOG_DEBUG("Content doesn't fit in window. Try flushing");
      // flush before write the remaining content
      if (FlushWriteBuffer() == false) {
        return false;
      }
    }
  }
  return true;
}

void LibeventSocket::CloseSocket() {
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

void LibeventSocket::Reset(short event_flags, LibeventThread *thread,
                           ConnState init_state) {
  is_disconnected = false;
  rbuf.Reset();
  wbuf.Reset();
  // TODO: Reuse packet manager, don't destroy
  pkt_manager.reset(nullptr);
  Init(event_flags, thread, init_state);
}

}  // End wire namespace
}  // End peloton namespace
