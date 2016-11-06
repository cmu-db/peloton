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

void LibeventSocket::Init(short event_flags, LibeventThread *thread,
                          ConnState init_state) {
  SetNonBlocking(sock_fd);
  SetTCPNoDelay(sock_fd);
  is_disconnected = false;
  is_started = false;

  this->event_flags = event_flags;
  this->thread = thread;
  this->state = init_state;

  // clear out packet
  rpkt.Reset();

  // TODO: Maybe switch to event_assign once State machine is implemented
  event = event_new(thread->GetEventBase(), sock_fd, event_flags,
                    EventHandler, this);
  event_add(event, nullptr);
}

void LibeventSocket::TransitState(ConnState next_state) {
  if (next_state != state)
    LOG_TRACE("conn %d transit to state %d", conn->sock_fd, (int)next_state);
  state = next_state;
}

// Update event
bool LibeventSocket::UpdateEvent(short flags) {
  auto base = thread->GetEventBase();
  if (event_del(event) == -1) {
    LOG_ERROR("Failed to delete event");
    return false;
  }
  auto result =
      event_assign(event, base, sock_fd, flags, EventHandler, (void *) this);

    if (result != 0) {
    LOG_ERROR("Failed to update event");
    return false;
  }

  event_flags = flags;

  if (event_add(event, nullptr) == -1) {
   LOG_ERROR("Failed to add event");
    return false;
  }

  return true;
}

void LibeventSocket::GetSizeFromPktHeader(size_t &start_index) {
  rpkt.len = 0;
  // directly converts from network byte order to little-endian
  for (size_t i=start_index; i<start_index+sizeof(uint32_t); i++) {
    rpkt.len = (rpkt.len << 8) | rbuf.GetByte(i);
  }
  // packet size includes initial bytes read as well
  rpkt.len = rpkt.len - sizeof(int32_t);
  rpkt.header_parsed = true;
}

bool LibeventSocket::IsReadDataAvailable(size_t bytes) {
  return ((rbuf.buf_ptr - 1) + bytes < rbuf.buf_size);
}

// The function tries to do a preliminary read to fetch the size value and
// then reads the rest of the packet.
// Assume: Packet length field is always 32-bit int
bool LibeventSocket::ReadPacketHeader() {
  size_t initial_read_size = sizeof(int32_t);
  if (pkt_manager.is_started == true) {
    // All packets other than the startup packet have a 5B header
    initial_read_size++;
  }
  // check if header bytes are available
  if (IsReadDataAvailable(initial_read_size) == false) {
    // nothing more to read
    return false;
  }

  // get packet size from the header
  if (is_started == true) {
    // Header also contains msg type
    rpkt.msg_type = rbuf.GetByte(rbuf.buf_ptr);
    // extract packet size
    GetSizeFromPktHeader(rbuf.buf_ptr+1);
  } else {
    GetSizeFromPktHeader(rbuf_.buf_ptr);
  }

  // we have processed the data, move buffer pointer
  rbuf.buf_ptr += initial_read_size;

  return true;
}

// Tries to read the contents of a single packet, returns true on success, false
// on failure.
bool LibeventSocket::ReadPacket() {
  if (IsReadDataAvailable(rpkt.len) == false) {
    // data not available yet, return
    return false;
  }

  // Initialize the packet's "contents"
  rpkt.InitializePacket(rbuf.buf_ptr, rbuf.Begin());

  // We have processed the data, move buffer pointer
  rbuf.buf_ptr += rpkt.len;

  return true;
}


ReadState LibeventSocket::FillReadBuffer() {
  ReadState result = READ_NO_DATA_RECEIVED;
  ssize_t bytes_read = 0;
  fd_set rset;
  bool done = false;

  // client has sent more data than it should?
  if (rbuf.buf_size - rbuf.buf_ptr == SOCKET_BUFFER_SIZE) {
    LOG_ERROR("Conn %d has exceeded read buffer size. Terminating.", sock_fd);
    TransitState(CONN_CLOSING);
  }

  // reset buffer if all the contents have been read
  if (rbuf.buf_ptr == rbuf.buf_size)
    rbuf.Reset();

  if (rbuf.buf_ptr > rbuf.buf_size) {
    LOG_WARN("ReadBuf ptr overflowed. This shouldn't happen!");
    rbuf.Reset();
  }

  /* Do we have leftover data and are we at the end of the buffer?
   * Move the data to the head of the buffer and clear out all the old data
   * Note: The assumption here is that all the packets/headers till
   *  rbuf.buf_ptr have been fully processed
   */
  if (rbuf.buf_ptr < rbuf.buf_size && rbuf.buf_size == SOCKET_BUFFER_SIZE) {
    // Move this data to the head of rbuf
    std::memmove(rbuf.GetPtr(0), rbuf.GetPtr(rbuf.buf_ptr),
                 rbuf.buf_size - rbuf.buf_ptr);
  }

  // return explicitly
  while (done == false) {
    if (rbuf.buf_size == SOCKET_BUFFER_SIZE) {
      // we have filled the whole buffer, exit loop
      done = true;
    } else {
      // try to fill the available space in the buffer
      bytes_read = read(sock_fd, &rbuf.buf[rbuf.buf_ptr],
                        SOCKET_BUFFER_SIZE - rbuf.buf_size);

      if (bytes_read > 0) {
        // read succeeded, update buffer size
        rbuf.buf_size += bytes_read;
        result = READ_DATA_RECEIVED;
      } else if (bytes_read == 0) {
        // Read failed
        return READ_ERROR;
      } else if (bytes_read < 0) {
        // related to non-blocking?
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // return whatever results we have
          LOG_DEBUG("Received: EAGAIN or EWOULDBLOCK");
          done = true;
        }

        if (errno == EINTR) {
          // interrupts are ok, try again
          LOG_DEBUG("Error Reading: EINTR");
          bytes_read = 0;
          continue;
        }

        // otherwise, we have some other error
        switch (errno) {
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

        // some other error occured
        return READ_ERROR;
      }
    }
  }
  return result;
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
  LOG_DEBUG("Attempt to close the connection %d", sock_fd);
  // Remove listening event
  event_del(event);

  TransitState(this, CONN_CLOSED);

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
