//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_connection.cpp
//
// Identification: src/network/network_connection.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include "network/postgres_protocol_handler.h"
#include "settings/settings_manager.h"
#include "network/network_connection.h"
#include "network/protocol_handler_factory.h"
#include "errno.h"
namespace peloton {
namespace network {

void NetworkConnection::Init(short event_flags, NetworkThread *thread,
                          ConnState init_state) {
  SetNonBlocking(sock_fd);
  SetTCPNoDelay(sock_fd);

  protocol_handler_ = nullptr;

  this->event_flags = event_flags;
  this->thread = thread;
  this->state = init_state;

  this->thread_id = thread->GetThreadID();

  // clear out packet
  if (network_event == nullptr) {
    network_event = event_new(thread->GetEventBase(), sock_fd, event_flags,
                      CallbackUtil::EventHandler, this);
  } else {
    // Reuse the event object if already initialized
    if (event_del(network_event) == -1) {
      LOG_ERROR("Failed to delete network event");
      PL_ASSERT(false);
    }

    auto result = event_assign(network_event, thread->GetEventBase(), sock_fd,
                               event_flags,
                               CallbackUtil::EventHandler, this);

    if (result != 0) {
      LOG_ERROR("Failed to update network event");
      PL_ASSERT(false);
    }
  }

  if (workpool_event == nullptr) {
    workpool_event = event_new(thread->GetEventBase(), -1, EV_PERSIST,
    CallbackUtil::EventHandler, this);
  } else {
    if (event_del(workpool_event) == -1) {
      LOG_ERROR("Failed to delete event");
      PL_ASSERT(false);
    }
    auto result = event_assign(workpool_event, thread->GetEventBase(), -1,
                                EV_PERSIST, CallbackUtil::EventHandler, this);
    if (result != 0) {
      LOG_ERROR("Failed to update workpool event");
      PL_ASSERT(false);
    }
  }

  event_add(network_event, nullptr);
  event_add(workpool_event, nullptr);

  //TODO:: should put the initialization else where.. check correctness first.
  traffic_cop_.SetTaskCallback(TriggerStateMachine, workpool_event);
}

void NetworkConnection::TriggerStateMachine(void* arg) {
  struct event* event = static_cast<struct event*>(arg);
  event_active(event, EV_WRITE, 0);
}

void NetworkConnection::TransitState(ConnState next_state) {
#ifdef LOG_TRACE_ENABLED
  if (next_state != state)
  LOG_TRACE("conn %d transit to state %d", sock_fd, (int)next_state);
#endif
  state = next_state;
}

// Update event
bool NetworkConnection::UpdateEvent(short flags) {
  auto base = thread->GetEventBase();
  if (event_del(network_event) == -1) {
    LOG_ERROR("Failed to delete event");
    return false;
  }
  auto result =
      event_assign(network_event, base, sock_fd, flags,
                   CallbackUtil::EventHandler, (void *)this);

  if (result != 0) {
    LOG_ERROR("Failed to update event");
    return false;
  }

  event_flags = flags;

  if (event_add(network_event, nullptr) == -1) {
    LOG_ERROR("Failed to add event");
    return false;
  }

  return true;
}


/**
 * Public Functions
 */

WriteState NetworkConnection::WritePackets() {
  // If I have data left in SSL buffer, before moving data into the local buffer,
  // send out the data in SSL buffer first.
  if (GetWriteBlocked()) {
    FlushWriteBuffer();
  }
  // iterate through all the packets
  for (; next_response_ < protocol_handler_->responses.size(); next_response_++) {
    auto pkt = protocol_handler_->responses[next_response_].get();
    LOG_TRACE("To send packet with type: %c, len %lu", static_cast<char>(pkt->msg_type), pkt->len);
    // write is not ready during write. transit to CONN_WRITE
    auto result = BufferWriteBytesHeader(pkt);
    if (result == WriteState::WRITE_NOT_READY || result == WriteState::WRITE_ERROR) return result;
    result = BufferWriteBytesContent(pkt);
    if (result == WriteState::WRITE_NOT_READY || result == WriteState::WRITE_ERROR) return result;
  }

  // Done writing all packets. clear packets
  protocol_handler_->responses.clear();
  next_response_ = 0;

  if (protocol_handler_->GetFlushFlag()) {
    return FlushWriteBuffer();
  }

  // we have flushed, disable force flush now
  protocol_handler_->SetFlushFlag(false);

  return WriteState::WRITE_COMPLETE;
}

ReadState NetworkConnection::FillReadBuffer() {
  ReadState result = ReadState::READ_NO_DATA_RECEIVED;
  ssize_t bytes_read = 0;
  bool done = false;
  // If partial SSL record exists in the SSL buffer, call SSL_read()
  // to read more data from the network buffer first.
  if (!GetReadBlocked()) {

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
        ERR_clear_error();
        // TODO(Yuchen): For the transparent negotiation to succeed, the ssl must have been
        // initialized to client or server mode?
        // Only when the whole SSL record has been received and processed completely,
        // SSL_read() will return reporting success.
        // Special case would be: SSL_read() reads the whole SSL record from the network buffer.
        // The network buffer becomes empty and data is in SSL buffer. We can't rely on
        // libevent read event since the system does not know about the SSL buffer. We need to
        // call SSL_pending() to check manually. (See StateMachine)
        SetReadBlockedOnWrite(false);
        SetReadBlocked(false);
        bytes_read = SSL_read(conn_SSL_context, rbuf_.GetPtr(rbuf_.buf_size),
                              rbuf_.GetMaxSize() - rbuf_.buf_size);
        LOG_TRACE("SSL read successfully");
        int err = SSL_get_error(conn_SSL_context, bytes_read);
        unsigned long ecode = (err != SSL_ERROR_NONE || bytes_read < 0) ? ERR_get_error() : 0;
        switch (err) {
          case SSL_ERROR_NONE: {
            // If successfully received, update buffer ptr and read status
            // keep reading till no data is available or the buffer becomes full
            rbuf_.buf_size += bytes_read;
            result = ReadState::READ_DATA_RECEIVED;
            break;
          }
          // The socket would have blocked when it's in blocking mode. It happens when one
          // SSL record arrives in multiple packets. Keep calling SSL_read until we receive all
          // the packets for the SSL record. Meanwhile do not move buffer ptr.
          // TODO(Yuchen): does libevent notifies if more data arrives? If that's the case, we don't
          // need to wait here. Actually, the buffer ptr is changed before the call..
          case SSL_ERROR_WANT_READ: {
            LOG_INFO("Fill read buffer, want read");
            SetReadBlocked(true);
            done = true;
            break;
          }
          // It happens when we're trying to rehandshake and we block on a write
          // during the handshake. We need to wait on the socket to be writable
          case SSL_ERROR_WANT_WRITE: {
            LOG_INFO("Fill read buffer, want write");
            SetReadBlockedOnWrite(true);
            done = true;
            break;
          }
          case SSL_ERROR_SYSCALL: {
            // if interrupted, try again
            if (errno == EINTR) {
              LOG_INFO("Error SSL Reading: EINTR");
              break;
            }
          }
          default: {
            LOG_ERROR("SSL read error: %d, error code: %lu", err, ecode);
            return ReadState::READ_ERROR;
          }
        }
      } else {
        bytes_read = read(sock_fd, rbuf_.GetPtr(rbuf_.buf_size),
                          rbuf_.GetMaxSize() - rbuf_.buf_size);
        LOG_TRACE("When filling read buffer, read %ld bytes", bytes_read);
        if (bytes_read > 0) {
          // read succeeded, update buffer size
          rbuf_.buf_size += bytes_read;
          result = ReadState::READ_DATA_RECEIVED;
        } else if (bytes_read == 0) {
          // Read failed, end of file
          return ReadState::READ_ERROR;
        } else if (bytes_read < 0) {
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
              case EBADF:LOG_TRACE("Error Reading: EBADF");
                break;
              case EDESTADDRREQ:LOG_TRACE("Error Reading: EDESTADDRREQ");
                break;
              case EDQUOT:LOG_TRACE("Error Reading: EDQUOT");
                break;
              case EFAULT:LOG_TRACE("Error Reading: EFAULT");
                break;
              case EFBIG:LOG_TRACE("Error Reading: EFBIG");
                break;
              case EINVAL:LOG_TRACE("Error Reading: EINVAL");
                break;
              case EIO:LOG_TRACE("Error Reading: EIO");
                break;
              case ENOSPC:LOG_TRACE("Error Reading: ENOSPC");
                break;
              case EPIPE:LOG_TRACE("Error Reading: EPIPE");
                break;
              default:LOG_TRACE("Error Reading: UNKNOWN");
            }
            // some other error occured
            return ReadState::READ_ERROR;
          }
        }
      }
    }
  }
  return result;
}

WriteState NetworkConnection::FlushWriteBuffer() {
  ssize_t written_bytes = 0;
  // while we still have outstanding bytes to write
  if (conn_SSL_context != nullptr) {
    while (wbuf_.buf_size > 0) {
      LOG_TRACE("SSL_write flush");
      ERR_clear_error();
      SetWriteBlocked(false);
      SetWriteBlockedOnRead(false);
      written_bytes = SSL_write(conn_SSL_context, &wbuf_.buf[wbuf_.buf_flush_ptr], wbuf_.buf_size);
      int err = SSL_get_error(conn_SSL_context, written_bytes);
      unsigned long ecode = (err != SSL_ERROR_NONE || written_bytes < 0) ? ERR_get_error() : 0;
      switch (err) {
        case SSL_ERROR_NONE: {
          wbuf_.buf_flush_ptr += written_bytes;
          wbuf_.buf_size -= written_bytes;
          break;
        }
        // We would have blocked on write if the socket is in blocking mode.
        // It happens when the server wants to send a large SSL record and the network buffer becomes full.
        // The kernel will flush the network buffer automatically. What we need to do is to call
        // SSL_write() again when the buffer becomes availble to write again(notified by Libevent). Now,
        // just return WRITE_NOT_READY and keeps the buffer ptr unchanged.
        // TODO(Yuchen): Change this. Can't write more packets and update buffer ptr when SSL_write() is not succeeded yet.
        case SSL_ERROR_WANT_WRITE: {
          SetWriteBlocked(true);
          LOG_TRACE("Flush write buffer, want write, not ready");
          return WriteState::WRITE_NOT_READY;
        }
        // It happens when doing rehandshake with client.
        case SSL_ERROR_WANT_READ: {
          SetWriteBlockedOnRead(true);
          LOG_TRACE("Flush write buffer, want read, not ready");
          return WriteState::WRITE_NOT_READY;
        }
        case SSL_ERROR_SYSCALL: {
          // If interrupted, try again.
          if (errno == EINTR) {
            LOG_TRACE("Flush write buffer, eintr");
            break;
          }
        }
        default: {
          LOG_ERROR("SSL write error: %d, error code: %lu", err, ecode);
          return WriteState::WRITE_ERROR;
        }
      }
    }
  } else {
    while (wbuf_.buf_size > 0) {
      written_bytes = 0;
      while (written_bytes <= 0) {
          LOG_TRACE("Normal write flush");
          written_bytes =
              write(sock_fd, &wbuf_.buf[wbuf_.buf_flush_ptr], wbuf_.buf_size);
        // Write failed
        if (written_bytes < 0) {
          switch (errno) {
            case EINTR:LOG_TRACE("Error Writing: EINTR");
              break;
            case EAGAIN:LOG_TRACE("Error Writing: EAGAIN");
              break;
            case EBADF:LOG_TRACE("Error Writing: EBADF");
              break;
            case EDESTADDRREQ:LOG_TRACE("Error Writing: EDESTADDRREQ");
              break;
            case EDQUOT:LOG_TRACE("Error Writing: EDQUOT");
              break;
            case EFAULT:LOG_TRACE("Error Writing: EFAULT");
              break;
            case EFBIG:LOG_TRACE("Error Writing: EFBIG");
              break;
            case EINVAL:LOG_TRACE("Error Writing: EINVAL");
              break;
            case EIO:LOG_TRACE("Error Writing: EIO");
              break;
            case ENOSPC:LOG_TRACE("Error Writing: ENOSPC");
              break;
            case EPIPE:LOG_TRACE("Error Writing: EPIPE");
              break;
            default:LOG_TRACE("Error Writing: UNKNOWN");
          }
          if (errno == EINTR) {
            // interrupts are ok, try again
            written_bytes = 0;
            continue;
            // Write would have blocked if the socket was
            // in blocking mode. Wait till it's readable
          } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Listen for socket being enabled for write
            if (!UpdateEvent(EV_WRITE | EV_PERSIST)) {
              return WriteState::WRITE_ERROR;
            }
            // We should go to CONN_WRITE state
            LOG_TRACE("WRITE NOT READY");
            return WriteState::WRITE_NOT_READY;
          } else {
            // fatal errors
            LOG_ERROR("Fatal error during write, errno %d", errno);
            return WriteState::WRITE_ERROR;
          }
        }

        // weird edge case?
        if (written_bytes == 0 && wbuf_.buf_size != 0) {
          LOG_TRACE("Not all data is written");
          continue;
        }
      }

      // update book keeping
      wbuf_.buf_flush_ptr += written_bytes;
      wbuf_.buf_size -= written_bytes;
    }
  }
  // buffer is empty
  wbuf_.Reset();

  // we are ok
  return WriteState::WRITE_COMPLETE;
}

std::string NetworkConnection::WriteBufferToString() {
  #ifdef LOG_TRACE_ENABLED
    LOG_TRACE("Write Buffer:");

    for (size_t i = 0; i < wbuf_.buf_size; ++i) {
      LOG_TRACE("%u", wbuf_.buf[i]);
    }
  #endif

  return std::string(wbuf_.buf.begin(), wbuf_.buf.end());
}


ProcessResult NetworkConnection::ProcessInitial() {
  //TODO: this is a direct copy and we should get rid of it later;

  if (initial_packet.header_parsed == false) {
    // parse out the header first
    if (ReadStartupPacketHeader(rbuf_, initial_packet) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }
  PL_ASSERT(initial_packet.header_parsed == true);

  if (initial_packet.is_initialized == false) {
    // packet needs to be initialized with rest of the contents
    //TODO: If other protocols are added, this need to be changed
    if (PostgresProtocolHandler::ReadPacket(rbuf_, initial_packet) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }
  //TODO: If other protocols are added, this need to be changed

  if (protocol_handler_ == nullptr) {
    protocol_handler_ =
      ProtocolHandlerFactory::CreateProtocolHandler(
          ProtocolHandlerType::Postgres, &traffic_cop_);
  }
  // We need to handle startup packet first
  //TODO: If other protocols are added, this need to be changed
  bool result = protocol_handler_->ProcessInitialPacket(&initial_packet, client_, ssl_able_, ssl_handshake_, finish_startup_packet_);
  // Clean up the initial_packet after finishing processing.
  initial_packet.Reset();
  if (result) {
    return ProcessResult::COMPLETE;
  } else {
    return ProcessResult::TERMINATE;
  }
}

// TODO: This function is now dedicated for postgres packet
bool NetworkConnection::ReadStartupPacketHeader(Buffer &rbuf, InputPacket &rpkt) {
  size_t initial_read_size = sizeof(int32_t);

  if (!rbuf.IsReadDataAvailable(initial_read_size)){
    return false;
  }

  // extract packet contents size
  //content lengths should exclude the length
  rpkt.len = rbuf.GetUInt32BigEndian() - sizeof(uint32_t);

  // do we need to use the extended buffer for this packet?
  rpkt.is_extended = (rpkt.len > rbuf.GetMaxSize());

  if (rpkt.is_extended) {
    LOG_TRACE("Using extended buffer for pkt size:%ld", rpkt.len);
    // reserve space for the extended buffer
    rpkt.ReserveExtendedBuffer();
  }

  // we have processed the data, move buffer pointer
  rbuf.buf_ptr += initial_read_size;
  rpkt.header_parsed = true;
  return true;
}

// Writes a packet's header (type, size) into the write buffer.
// Return false when the socket is not ready for write
WriteState NetworkConnection::BufferWriteBytesHeader(OutputPacket *pkt) {
  // If we should not write
  if (pkt->skip_header_write) {
    return WriteState::WRITE_COMPLETE;
  }

  size_t len = pkt->len;
  unsigned char type = static_cast<unsigned char>(pkt->msg_type);
  int len_nb;  // length in network byte order

  // check if we have enough space in the buffer
  if (wbuf_.GetMaxSize() - wbuf_.buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    auto result = FlushWriteBuffer();
    if (result == WriteState::WRITE_NOT_READY || result == WriteState::WRITE_ERROR) {
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

  if (finish_startup_packet_) {
    // append the bytes of this integer in network-byte order
    std::copy(reinterpret_cast<uchar *>(&len_nb),
              reinterpret_cast<uchar *>(&len_nb) + 4,
              std::begin(wbuf_.buf) + wbuf_.buf_ptr);

    // move the write buffer pointer and update size of the socket buffer
    wbuf_.buf_ptr += sizeof(int32_t);
  }
  wbuf_.buf_size = wbuf_.buf_ptr;

  // Header is written to socket buf. No need to write it in the future
  pkt->skip_header_write = true;
  return WriteState::WRITE_COMPLETE;
}

// Writes a packet's content into the write buffer
// Return false when the socket is not ready for write
WriteState NetworkConnection::BufferWriteBytesContent(OutputPacket *pkt) {
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
      return WriteState::WRITE_COMPLETE;
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
      if (result == WriteState::WRITE_NOT_READY || result == WriteState::WRITE_ERROR) {
        // need to retry or close connection
        return result;
      }
    }
  }
  return WriteState::WRITE_COMPLETE;
}

void NetworkConnection::CloseSocket() {
  LOG_DEBUG("Attempt to close the connection %d", sock_fd);
  // Remove listening event
  event_del(network_event);
  event_del(workpool_event);
  // event_free(event);
  TransitState(ConnState::CONN_CLOSED);
  if (conn_SSL_context != nullptr) {
    int shutdown_ret = 0;
    while (1) {
      ERR_clear_error();
      shutdown_ret = SSL_shutdown(conn_SSL_context);
      int err = SSL_get_error(conn_SSL_context, shutdown_ret);
      if (shutdown_ret == 1) {
        break;
      } else if (shutdown_ret == 0) {
        LOG_TRACE("SSL shutdown is not finished yet");
        continue;
      } else {
        if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
          continue;
        } else {
          LOG_ERROR("Error shutting down ssl session, err: %d", err);
          break;
        }
      }
    }
  }
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

void NetworkConnection::Reset() {
  client_.Reset();
  rbuf_.Reset();
  wbuf_.Reset();
  // The listening connection do not have protocol handler
  if (protocol_handler_ != nullptr) {
    protocol_handler_->Reset();
  }
  state = ConnState::CONN_INVALID;
  traffic_cop_.Reset();
  next_response_ = 0;
  ssl_handshake_ = false;
  finish_startup_packet_ = false;
  initial_packet.Reset();
  if (conn_SSL_context != nullptr) {
    SSL_free(conn_SSL_context);
    conn_SSL_context = nullptr;
  }
  SetWriteBlockedOnRead(false);
  SetReadBlockedOnWrite(false);
  SetReadBlocked(false);
  SetWriteBlocked(false);
}

void NetworkConnection::StateMachine(NetworkConnection *conn) {
  bool done = false;

  while (done == false) {
    LOG_TRACE("current state: %d", (int)conn->state);
    switch (conn->state) {
      case ConnState::CONN_LISTENING: {
        struct sockaddr_storage addr;
        socklen_t addrlen = sizeof(addr);
        int new_conn_fd =
            accept(conn->sock_fd, (struct sockaddr *)&addr, &addrlen);
        if (new_conn_fd == -1) {
          LOG_ERROR("Failed to accept");
        }
        (static_cast<NetworkMasterThread *>(conn->thread))
            ->DispatchConnection(new_conn_fd, EV_READ | EV_PERSIST);
        done = true;
        break;
      }

      case ConnState::CONN_READ: {
        auto res = conn->FillReadBuffer();
        switch (res) {
          case ReadState::READ_DATA_RECEIVED:
            // wait for some other event
            if (!conn->finish_startup_packet_) {
              conn->TransitState(ConnState::CONN_PROCESS_INITIAL);
            }
            else {
              conn->TransitState(ConnState::CONN_PROCESS);
            }
            break;

          case ReadState::READ_NO_DATA_RECEIVED:
            // process what we read
            conn->TransitState(ConnState::CONN_WAIT);
            break;

          case ReadState::READ_ERROR:
            // fatal error for the connection
            conn->TransitState(ConnState::CONN_CLOSING);
            break;
        }
        break;
      }

      case ConnState::CONN_WAIT: {
        if (conn->UpdateEvent(EV_READ | EV_PERSIST) == false) {
          LOG_ERROR("Failed to update event, closing");
          conn->TransitState(ConnState::CONN_CLOSING);
          break;
        }

        conn->TransitState(ConnState::CONN_READ);
        // SSL_pending() checks whether there is data left in SSL buffer.
        // It does not work reliably when doing handshake.
        // If SSL session doing rehandshake or there is not data left in SSL buffer,
        // exit the loop and wait for event trigger.
        if (conn->conn_SSL_context == nullptr || conn->GetReadBlockedOnWrite() ||
            conn->GetReadBlockedOnWrite() || !SSL_pending(conn->conn_SSL_context)) {
          done = true;
        }
        break;
      }

      case ConnState::CONN_PROCESS_INITIAL: {
        if (conn->ssl_handshake_) {
          // start SSL handshake
          // TODO: consider free conn_SSL_context
          conn->conn_SSL_context = SSL_new(NetworkManager::ssl_context);
          SSL_set_session_id_context(conn->conn_SSL_context, nullptr, 0);
          if (SSL_set_fd(conn->conn_SSL_context, conn->sock_fd) == 0) {
            LOG_ERROR("Failed to set SSL fd");
            PL_ASSERT(false);
          }

          bool handshake_fail = false;
          //TODO(Yuchen): post-connection verification?
          while (!handshake_fail) {
            //clear current thread's error queue before any OpenSSL call
            ERR_clear_error();
            int ssl_accept_ret = SSL_accept(conn->conn_SSL_context);
            if (ssl_accept_ret > 0) {
              break;
            }
            int err = SSL_get_error(conn->conn_SSL_context, ssl_accept_ret);
            int ecode = ERR_get_error();
            char error_string[120];
            ERR_error_string(ecode, error_string);
            switch (err) {
              case SSL_ERROR_SSL: {
                if (ecode < 0) {
                  LOG_ERROR("Could not accept SSL connection");
                } else {
                  LOG_ERROR("Could not accept SSL connection: EOF detected, ssl_error_ssl, %s", error_string);
                }
                handshake_fail = true;
                break;
              }
              case SSL_ERROR_ZERO_RETURN: {
                LOG_ERROR("Could not accept SSL connection: EOF detected, ssl_error_zero_return, %s", error_string);
                handshake_fail = true;
                break;
              }
              case SSL_ERROR_SYSCALL: {
                if (ecode < 0) {
                  LOG_ERROR("Could not accept SSL connection, %s", error_string);
                } else {
                  LOG_ERROR("Could not accept SSL connection: EOF detected, ssl_sys_call, %s", error_string);
                }
                handshake_fail = true;
                break;
              }
              case SSL_ERROR_WANT_READ:
              case SSL_ERROR_WANT_WRITE:break;
              default: {
                LOG_ERROR("Unrecognized SSL error code: %d", err);
                handshake_fail = true;
              }
            }
          }
          if (!handshake_fail) {
            // handshake succeeds, reset ssl_sent_
            conn->ssl_handshake_ = false;
            conn->TransitState(ConnState::CONN_PROCESS_INITIAL);
          } else {
            // TODO(Yuchen): Handle the case when ssl handshake fails,
            // shall we close the connection directly?
            conn->ssl_handshake_ = false;
            conn->TransitState(ConnState::CONN_CLOSING);
          }
        }

        switch (conn->ProcessInitial()) {
          case ProcessResult::COMPLETE: {
            conn->TransitState(ConnState::CONN_WRITE);
            break;
          }
          case ProcessResult::MORE_DATA_REQUIRED: {
            conn->TransitState(ConnState::CONN_WAIT);
            break;
          }
          case ProcessResult::TERMINATE: {
            conn->TransitState(ConnState::CONN_CLOSING);
            break;
          }
          default: // PROCESSING is impossible to happens in initial packets
            break;
        }
        break;
      }

      case ConnState::CONN_PROCESS: {
        ProcessResult status;

        status = conn->protocol_handler_->Process(conn->rbuf_,
                                                        (size_t) conn->thread_id);

        switch (status) {
          case ProcessResult::MORE_DATA_REQUIRED:
            conn->TransitState(ConnState::CONN_WAIT);
            break;
          case ProcessResult::TERMINATE:
            // packet processing can't proceed further
            conn->TransitState(ConnState::CONN_CLOSING);
            break;
          case ProcessResult::COMPLETE:
            // We should have responses ready to send
            conn->TransitState(ConnState::CONN_WRITE);
            break;
          case ProcessResult::PROCESSING: {
            if (event_del(conn->network_event) == -1) {
              //TODO: There may be better way to handle this error
              LOG_ERROR("Failed to delete event");
              PL_ASSERT(false);
            }
            LOG_TRACE("ProcessResult: queueing");
            conn->TransitState(ConnState::CONN_GET_RESULT);
            done = true;
            break;
          }
        }

        break;
      }

      case ConnState::CONN_GET_RESULT: {
        if (event_add(conn->network_event, nullptr) < 0) {
          LOG_ERROR("Failed to add event");
          PL_ASSERT(false);
        }
        conn->protocol_handler_->GetResult();
        conn->traffic_cop_.SetQueuing(false);
        conn->TransitState(ConnState::CONN_WRITE);
        break;
      }

      case ConnState::CONN_WRITE: {
        // examine write packets result
        switch (conn->WritePackets()) {
          case WriteState::WRITE_COMPLETE: {
            // TODO(Yuchen): We need to check the update events carefully.
            if (!conn->UpdateEvent(EV_READ | EV_PERSIST)) {
              LOG_ERROR("Failed to update event, closing");
              conn->TransitState(ConnState::CONN_CLOSING);
              break;
            }
            if (!conn->finish_startup_packet_) {
              conn->TransitState(ConnState::CONN_PROCESS_INITIAL);
            }
            else {
              conn->TransitState(ConnState::CONN_PROCESS);
            }
            break;
          }

          case WriteState::WRITE_NOT_READY: {
            // we can't write right now. Exit state machine
            // and wait for next callback
            done = true;
            break;
          }

          case WriteState::WRITE_ERROR: {
            LOG_ERROR("Error during write, closing connection");
            conn->TransitState(ConnState::CONN_CLOSING);
            break;
          }
        }
        break;
      }

      case ConnState::CONN_CLOSING: {
        conn->CloseSocket();
        done = true;
        break;
      }

      case ConnState::CONN_CLOSED: {
        done = true;
        break;
      }

      case ConnState::CONN_INVALID: {
        PL_ASSERT(false);
        break;
      }
    }
  }
  LOG_TRACE("END of while loop");
}

}  // namespace network
}  // namespace peloton
