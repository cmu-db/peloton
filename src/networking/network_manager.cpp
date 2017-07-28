//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_socket.cpp
//
// Identification: src/networking/network_socket.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include "networking/network_manager.h"

namespace peloton {
namespace networking {

void NetworkManager::Init(short event_flags, NetworkThread *thread,
                          ConnState init_state) {
  SetNonBlocking(sock_fd);
  SetTCPNoDelay(sock_fd);

  this->event_flags = event_flags;
  this->thread = thread;
  this->state = init_state;

  this->thread_id = thread->GetThreadID();

  // clear out packet
  if (event == nullptr) {
    event = event_new(thread->GetEventBase(), sock_fd, event_flags,
                      EventHandler, this);
  } else {
    // Reuse the event object if already initialized
    if (event_del(event) == -1) {
      LOG_ERROR("Failed to delete event");
      PL_ASSERT(false);
    }

    auto result = event_assign(event, thread->GetEventBase(), sock_fd,
                               event_flags, EventHandler, this);

    if (result != 0) {
      LOG_ERROR("Failed to update event");
      PL_ASSERT(false);
    }
  }
  event_add(event, nullptr);
}

void NetworkManager::TransitState(ConnState next_state) {
  if (next_state != state)
    LOG_TRACE("conn %d transit to state %d", sock_fd, (int)next_state);
  state = next_state;
}

// Update event
bool NetworkManager::UpdateEvent(short flags) {
  auto base = thread->GetEventBase();
  if (event_del(event) == -1) {
    LOG_ERROR("Failed to delete event");
    return false;
  }
  auto result =
      event_assign(event, base, sock_fd, flags, EventHandler, (void *)this);

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


/**
 * Public Functions
 */

WriteState NetworkManager::WritePackets() {
  // iterate through all the packets
  for (; next_response_ < protocol_handler_.responses.size(); next_response_++) {
    auto pkt = protocol_handler_.responses[next_response_].get();
    LOG_TRACE("To send packet with type: %c", static_cast<char>(pkt->msg_type));
    // write is not ready during write. transit to CONN_WRITE
    auto result = BufferWriteBytesHeader(pkt);
    if (result == WRITE_NOT_READY || result == WRITE_ERROR) return result;
    result = BufferWriteBytesContent(pkt);
    if (result == WRITE_NOT_READY || result == WRITE_ERROR) return result;
  }

  // Done writing all packets. clear packets
  protocol_handler_.responses.clear();
  next_response_ = 0;

  if (protocol_handler_.force_flush == true) {
    return FlushWriteBuffer();
  }
  return WRITE_COMPLETE;
}

ReadState NetworkManager::FillReadBuffer() {
  ReadState result = READ_NO_DATA_RECEIVED;
  ssize_t bytes_read = 0;
  bool done = false;

  // reset buffer if all the contents have been read
  if (rbuf_.buf_ptr == rbuf_.buf_size) rbuf_.Reset();

  // buf_ptr shouldn't overflow
  PL_ASSERT(rbuf_.buf_ptr <= rbuf_.buf_size);

  /* Do we have leftover data and are we at the end of the buffer?
   * Move the data to the head of the buffer and clear out all the old data
   * Note: The assumption here is that all the packets/headers till
   *  rbuf_.buf_ptr have been fully processed
   */
  if (rbuf_.buf_ptr < rbuf_.buf_size && rbuf_.buf_size == rbuf_.GetMaxSize()) {
    auto unprocessed_len = rbuf_.buf_size - rbuf_.buf_ptr;
    // Move this data to the head of rbuf_1
    std::memmove(rbuf_.GetPtr(0), rbuf_.GetPtr(rbuf_.buf_ptr), unprocessed_len);
    // update pointers
    rbuf_.buf_ptr = 0;
    rbuf_.buf_size = unprocessed_len;
  }

  // return explicitly
  while (done == false) {
    if (rbuf_.buf_size == rbuf_.GetMaxSize()) {
      // we have filled the whole buffer, exit loop
      done = true;
    } else {
      // try to fill the available space in the buffer
      // if the connection is a SSL connection, we use SSL_read, otherwise
      // we use general read function
      if (conn_SSL_context != nullptr) {
        bytes_read = SSL_read(conn_SSL_context, rbuf_.GetPtr(rbuf_.buf_size),
                                rbuf_.GetMaxSize() - rbuf_.buf_size);
      }
      else {
        bytes_read = read(sock_fd, rbuf_.GetPtr(rbuf_.buf_size),
                        rbuf_.GetMaxSize() - rbuf_.buf_size);
      }

      if (bytes_read > 0) {
        // read succeeded, update buffer size
        rbuf_.buf_size += bytes_read;
        result = READ_DATA_RECEIVED;
      } else if (bytes_read == 0) {
        // Read failed
        return READ_ERROR;
      } else if (bytes_read < 0) {
        // related to non-blocking?
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // return whatever results we have
          LOG_TRACE("Received: EAGAIN or EWOULDBLOCK");
          done = true;
        } else if (errno == EINTR) {
          // interrupts are ok, try again
          LOG_TRACE("Error Reading: EINTR");
          continue;
        } else {
          // otherwise, we have some other error
          switch (errno) {
            case EBADF:
              LOG_TRACE("Error Reading: EBADF");
              break;
            case EDESTADDRREQ:
              LOG_TRACE("Error Reading: EDESTADDRREQ");
              break;
            case EDQUOT:
              LOG_TRACE("Error Reading: EDQUOT");
              break;
            case EFAULT:
              LOG_TRACE("Error Reading: EFAULT");
              break;
            case EFBIG:
              LOG_TRACE("Error Reading: EFBIG");
              break;
            case EINVAL:
              LOG_TRACE("Error Reading: EINVAL");
              break;
            case EIO:
              LOG_TRACE("Error Reading: EIO");
              break;
            case ENOSPC:
              LOG_TRACE("Error Reading: ENOSPC");
              break;
            case EPIPE:
              LOG_TRACE("Error Reading: EPIPE");
              break;
            default:
              LOG_TRACE("Error Reading: UNKNOWN");
          }
          // some other error occured
          return READ_ERROR;
        }
      }
    }
  }
  return result;
}

WriteState NetworkManager::FlushWriteBuffer() {
  ssize_t written_bytes = 0;
  // while we still have outstanding bytes to write
  while (wbuf_.buf_size > 0) {
    written_bytes = 0;
    while (written_bytes <= 0) {
      if (conn_SSL_context != nullptr) {
        written_bytes =
            SSL_write(conn_SSL_context, &wbuf_.buf[wbuf_.buf_flush_ptr], wbuf_.buf_size);
      }
      else {
        written_bytes =
           write(sock_fd, &wbuf_.buf[wbuf_.buf_flush_ptr], wbuf_.buf_size);
      }
      // Write failed
      if (written_bytes < 0) {
        switch (errno) {
          case EINTR:
            LOG_TRACE("Error Writing: EINTR");
            break;
          case EAGAIN:
            LOG_TRACE("Error Writing: EAGAIN");
            break;
          case EBADF:
            LOG_TRACE("Error Writing: EBADF");
            break;
          case EDESTADDRREQ:
            LOG_TRACE("Error Writing: EDESTADDRREQ");
            break;
          case EDQUOT:
            LOG_TRACE("Error Writing: EDQUOT");
            break;
          case EFAULT:
            LOG_TRACE("Error Writing: EFAULT");
            break;
          case EFBIG:
            LOG_TRACE("Error Writing: EFBIG");
            break;
          case EINVAL:
            LOG_TRACE("Error Writing: EINVAL");
            break;
          case EIO:
            LOG_TRACE("Error Writing: EIO");
            break;
          case ENOSPC:
            LOG_TRACE("Error Writing: ENOSPC");
            break;
          case EPIPE:
            LOG_TRACE("Error Writing: EPIPE");
            break;
          default:
            LOG_TRACE("Error Writing: UNKNOWN");
        }
        if (errno == EINTR) {
          // interrupts are ok, try again
          written_bytes = 0;
          continue;
          // Write would have blocked if the socket was
          // in blocking mode. Wait till it's readable
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // Listen for socket being enabled for write
          UpdateEvent(EV_WRITE | EV_PERSIST);
          // We should go to CONN_WRITE state
          return WRITE_NOT_READY;
        } else {
          // fatal errors
          LOG_ERROR("Fatal error during write");
          return WRITE_ERROR;
        }
      }

      // weird edge case?
      if (written_bytes == 0 && wbuf_.buf_size != 0) {
        LOG_DEBUG("Not all data is written");
        continue;
      }
    }

    // update book keeping
    wbuf_.buf_flush_ptr += written_bytes;
    wbuf_.buf_size -= written_bytes;
  }

  // buffer is empty
  wbuf_.Reset();

  // we have flushed, disable force flush now
  protocol_handler_.force_flush = false;

  // we are ok
  return WRITE_COMPLETE;
}

/*
 * process_startup_packet - Processes the startup packet
 *  (after the size field of the header).
 */
int NetworkManager::ProcessInitialPacket() {
  std::string token, value;

  int32_t available_len = rbuf_.buf_size - rbuf_.buf_ptr;

  if (available_len < sizeof(int32_t)) {
    return 0;
  }

  // TODO: this is a direct copy from Postgres's packet manager
  int32_t required_len = 0;
  size_t cursor = rbuf_.buf_ptr;
  // directly converts from network byte order to little-endian
  for (size_t i = cursor; i < cursor + sizeof(uint32_t); i++) {
    required_len = (required_len << 8) | rbuf_.GetByte(i);
  }

  cursor += sizeof(uint32_t);

  if (available_len < required_len) {
    return 0;
  }

  int32_t proto_version = 0;

  for (size_t i = cursor; i < sizeof(uint32_t); i++) {
     proto_version= (proto_version << 8) | rbuf_.GetByte(i);
  }

  cursor += sizeof(uint32_t);


  std::string contents =std::string(rbuf_.Begin() + cursor,
                                    rbuf_.Begin() + cursor - sizeof(uint32_t) + required_len);

  LOG_INFO("protocol version: %d", proto_version);
  bool res;
  int res_base = 0;


  // TODO: consider more about return value
  if (proto_version == SSL_MESSAGE_VERNO) {
    res = ProcessSSLRequestPacket();
    if (!res)
      res_base = 0;
    else
      res_base = -1;
  }
  else {
    res = ProcessStartupPacket(contents, proto_version);
    if (!res)
      res_base = 0;
    else
      res_base = 1;
  }

  return res_base;
}

bool NetworkManager::ProcessStartupPacket(std::string& contents, int32_t proto_version) {
  std::string token, value;
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  // Only protocol version 3 is supported
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    LOG_ERROR("Protocol error: Only protocol version 3 is supported.");
    exit(EXIT_FAILURE);
  }

  std::vector<std::string> tokens;
  boost::split(tokens, contents, boost::is_any_of('\0'));

  // TODO: check for more malformed cases
  // iterate till the end
  for (int i = 0 ; i < tokens.size(); i++) {
    token = tokens[i];
    // if the option database was found
    if (token.compare("database") == 0) {
      client_.dbname = tokens[++i];
    } else if (token.compare(("user")) == 0) {
      client_.user = tokens[++i];
    } else {
      client_.cmdline_options[token] = tokens[++i];
    }
  }

  // send auth-ok ('R')
  response->msg_type = NetworkMessageType::AUTHENTICATION_REQUEST;
  PacketPutInt(response.get(), 0, 4);
  responses.push_back(std::move(response));

  // Send the parameterStatus map ('S')
  for (auto it = parameter_status_map_.begin();
       it != parameter_status_map_.end(); it++) {
    MakeHardcodedParameterStatus(*it);
  }

  // ready-for-query packet -> 'Z'
  SendReadyForQuery(NetworkTransactionStateType::IDLE);

  // we need to send the response right away
  force_flush = true;

  return true;
}

bool NetworkManager::ProcessSSLRequestPacket() {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  // TODO: consider more about a proper response
  response->msg_type = NetworkMessageType::SSL_YES;
  responses.push_back(std::move(response));
  force_flush = true;
  return true;
}

void NetworkManager::PrintWriteBuffer() {
  LOG_TRACE("Write Buffer:");

  for (size_t i = 0; i < wbuf_.buf_size; ++i) {
    LOG_TRACE("%u", wbuf_.buf[i]);
  }
}

// Writes a packet's header (type, size) into the write buffer.
// Return false when the socket is not ready for write
WriteState NetworkManager::BufferWriteBytesHeader(OutputPacket *pkt) {
  // If we should not write
  if (pkt->skip_header_write) {
    return WRITE_COMPLETE;
  }

  size_t len = pkt->len;
  unsigned char type = static_cast<unsigned char>(pkt->msg_type);
  int len_nb;  // length in network byte order

  // check if we have enough space in the buffer
  if (wbuf_.GetMaxSize() - wbuf_.buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    auto result = FlushWriteBuffer();
    if (result == WRITE_NOT_READY || result == WRITE_ERROR) {
      // Socket is not ready for write
      return result;
    }
  }

  // assuming wbuf is now large enough to fit type and size fields in one go
  if (type != 0) {
    // type shouldn't be ignored
    wbuf_.buf[wbuf_.buf_ptr++] = type;
  }

  // make len include its field size as well
  len_nb = htonl(len + sizeof(int32_t));

  // append the bytes of this integer in network-byte order
  std::copy(reinterpret_cast<uchar *>(&len_nb),
            reinterpret_cast<uchar *>(&len_nb) + 4,
            std::begin(wbuf_.buf) + wbuf_.buf_ptr);

  // move the write buffer pointer and update size of the socket buffer
  wbuf_.buf_ptr += sizeof(int32_t);
  wbuf_.buf_size = wbuf_.buf_ptr;

  // Header is written to socket buf. No need to write it in the future
  pkt->skip_header_write = true;
  return WRITE_COMPLETE;
}

// Writes a packet's content into the write buffer
// Return false when the socket is not ready for write
WriteState NetworkManager::BufferWriteBytesContent(OutputPacket *pkt) {
  // the packet content to write
  ByteBuf &pkt_buf = pkt->buf;
  // the length of remaining content to write
  size_t len = pkt->len;
  // window is the size of remaining space in socket's wbuf
  size_t window = 0;

  // fill the contents
  while (len) {
    // calculate the remaining space in wbuf
    window = wbuf_.GetMaxSize() - wbuf_.buf_ptr;
    if (len <= window) {
      // contents fit in the window, range copy "len" bytes
      std::copy(std::begin(pkt_buf) + pkt->write_ptr,
                std::begin(pkt_buf) + pkt->write_ptr + len,
                std::begin(wbuf_.buf) + wbuf_.buf_ptr);

      // Move the cursor and update size of socket buffer
      wbuf_.buf_ptr += len;
      wbuf_.buf_size = wbuf_.buf_ptr;
      LOG_TRACE("Content fit in window. Write content successful");
      return WRITE_COMPLETE;
    } else {
      // contents longer than socket buffer size, fill up the socket buffer
      // with "window" bytes

      std::copy(std::begin(pkt_buf) + pkt->write_ptr,
                std::begin(pkt_buf) + pkt->write_ptr + window,
                std::begin(wbuf_.buf) + wbuf_.buf_ptr);

      // move the packet's cursor
      pkt->write_ptr += window;
      len -= window;
      // Now the wbuf is full
      wbuf_.buf_size = wbuf_.GetMaxSize();

      LOG_TRACE("Content doesn't fit in window. Try flushing");
      auto result = FlushWriteBuffer();
      // flush before write the remaining content
      if (result == WRITE_NOT_READY || result == WRITE_ERROR) {
        // need to retry or close connection
        return result;
      }
    }
  }
  return WRITE_COMPLETE;
}

void NetworkManager::CloseSocket() {
  LOG_DEBUG("Attempt to close the connection %d", sock_fd);
  // Remove listening event
  event_del(event);
  // event_free(event);

  TransitState(CONN_CLOSED);
  Reset();
  for (;;) {
    int status = close(sock_fd);
    if (status < 0) {
      // failed close
      if (errno == EINTR) {
        // interrupted, try closing again
        continue;
      }
    }
    LOG_DEBUG("Already Closed the connection %d", sock_fd);
    return;
  }
}

void NetworkManager::Reset() {
  rbuf_.Reset();
  wbuf_.Reset();
  protocol_handler_.Reset();
  state = CONN_INVALID;
  rpkt.Reset();
  next_response_ = 0;
}

}  // End networking namespace
}  // End peloton namespace
