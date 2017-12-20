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
#include <include/network/postgres_protocol_handler.h>
#include "network/network_connection.h"
#include "network/protocol_handler_factory.h"
#include "network/network_callback_util.h"
#include "network/connection_dispatcher_task.h"
#include "network/connection_handle.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) x >> 16
#define UNUSED(x) (void)(x)
namespace peloton {
namespace network {

void NetworkConnection::Init(short event_flags, NotifiableTask *handler) {
  SetNonBlocking(sock_fd_);
  SetTCPNoDelay(sock_fd_);

  protocol_handler_ = nullptr;
  this->handler = handler;

  if (network_event != nullptr)
    handler->UpdateEvent(network_event, sock_fd_, event_flags, CallbackUtil::OnNetworkEvent, this);
  else
    network_event = handler->RegisterEvent(sock_fd_, event_flags, CallbackUtil::OnNetworkEvent, this);

  if (workpool_event != nullptr)
    handler->UpdateManualEvent(workpool_event, CallbackUtil::OnNetworkEvent, this);
  else
    workpool_event = handler->RegisterManualEvent(CallbackUtil::OnNetworkEvent, this);

  //TODO:: should put the initialization else where.. check correctness first.
  traffic_cop_.SetTaskCallback([](void *arg) {
    struct event* event = static_cast<struct event*>(arg);
    event_active(event, EV_WRITE, 0);
  }, workpool_event);

  state_machine_ = ConnectionHandleStateMachine(ConnState::CONN_READ);
}

// Update event
bool NetworkConnection::UpdateEvent(short flags) {
  handler->UpdateEvent(network_event, sock_fd_, flags, CallbackUtil::OnNetworkEvent, this);
  // TODO(tianyu) Not propagate error since we will change to exceptions anyway.
  return true;
}

/**
 * Public Functions
 */
WriteState NetworkConnection::WritePackets() {
  // iterate through all the packets
  for (; next_response_ < protocol_handler_->responses.size(); next_response_++) {
    auto pkt = protocol_handler_->responses[next_response_].get();
    LOG_TRACE("To send packet with type: %c", static_cast<char>(pkt->msg_type));
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

Transition NetworkConnection::FillReadBuffer() {
  Transition result = Transition::NEED_DATA;
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
        bytes_read = read(sock_fd_, rbuf_.GetPtr(rbuf_.buf_size),
                        rbuf_.GetMaxSize() - rbuf_.buf_size);
        LOG_TRACE("When filling read buffer, read %ld bytes", bytes_read);
      }

      if (bytes_read > 0) {
        // read succeeded, update buffer size
        rbuf_.buf_size += bytes_read;
        result = Transition::PROCEED;
      } else if (bytes_read == 0) {
        // Read failed
        return Transition::ERROR;
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
          return Transition::ERROR;
        }
      }
    }
  }
  return result;
}

WriteState NetworkConnection::FlushWriteBuffer() {
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
           write(sock_fd_, &wbuf_.buf[wbuf_.buf_flush_ptr], wbuf_.buf_size);
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
          if (!UpdateEvent(EV_WRITE | EV_PERSIST)) {
            return WriteState::WRITE_ERROR;
          }
          // We should go to CONN_WRITE state
          LOG_DEBUG("WRITE NOT READY");
          return WriteState::WRITE_NOT_READY;
        } else {
          // fatal errors
          LOG_ERROR("Fatal error during write, errno %d", errno);
          return WriteState::WRITE_ERROR;
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
  InputPacket rpkt;

  if (rpkt.header_parsed == false) {
    // parse out the header first
    if (ReadStartupPacketHeader(rbuf_, rpkt) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }
  PL_ASSERT(rpkt.header_parsed == true);

  if (rpkt.is_initialized == false) {
    // packet needs to be initialized with rest of the contents
    //TODO: If other protocols are added, this need to be changed
    if (PostgresProtocolHandler::ReadPacket(rbuf_, rpkt) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }

  // We need to handle startup packet first

  if (!ProcessInitialPacket(&rpkt)) {
    return ProcessResult::TERMINATE;
  }
  return ProcessResult::COMPLETE;
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
    LOG_DEBUG("Using extended buffer for pkt size:%ld", rpkt.len);
    // reserve space for the extended buffer
    rpkt.ReserveExtendedBuffer();
  }

  // we have processed the data, move buffer pointer
  rbuf.buf_ptr += initial_read_size;
  rpkt.header_parsed = true;
  return true;
}

/*
 * process_startup_packet - Processes the startup packet
 *  (after the size field of the header).
 */
bool NetworkConnection::ProcessInitialPacket(InputPacket *pkt) {
  std::string token, value;
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  int32_t proto_version = PacketGetInt(pkt, sizeof(int32_t));
  LOG_INFO("protocol version: %d", proto_version);

  // TODO: consider more about return value
  if (proto_version == SSL_MESSAGE_VERNO) {
    LOG_TRACE("process SSL MESSAGE");
    return ProcessSSLRequestPacket(pkt);
  }
  else {
    LOG_TRACE("process startup packet");
    return ProcessStartupPacket(pkt, proto_version);
  }
}

bool NetworkConnection::ProcessSSLRequestPacket(InputPacket *pkt) {
  UNUSED(pkt);
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  // TODO: consider more about a proper response
  response->msg_type = NetworkMessageType::SSL_YES;
  protocol_handler_->responses.push_back(std::move(response));
  protocol_handler_->SetFlushFlag(true);
  ssl_sent_ = true;
  return true;
}

bool NetworkConnection::ProcessStartupPacket(InputPacket* pkt, int32_t proto_version) {
  std::string token, value;


  // Only protocol version 3 is supported
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    LOG_ERROR("Protocol error: Only protocol version 3 is supported.");
    exit(EXIT_FAILURE);
  }

  // TODO: check for more malformed cases
  // iterate till the end
  for (;;) {
    // loop end case?
    if (pkt->ptr >= pkt->len) break;
    GetStringToken(pkt, token);
    // if the option database was found
    if (token.compare("database") == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client_.dbname);
    } else if (token.compare(("user")) == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client_.user);
    }
    else {
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, value);
      client_.cmdline_options[token] = value;
    }

  }

  protocol_handler_ =
      ProtocolHandlerFactory::CreateProtocolHandler(
          ProtocolHandlerType::Postgres, &traffic_cop_);

  protocol_handler_->SendInitialResponse();

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

  // append the bytes of this integer in network-byte order
  std::copy(reinterpret_cast<uchar *>(&len_nb),
            reinterpret_cast<uchar *>(&len_nb) + 4,
            std::begin(wbuf_.buf) + wbuf_.buf_ptr);

  // move the write buffer pointer and update size of the socket buffer
  wbuf_.buf_ptr += sizeof(int32_t);
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

Transition NetworkConnection::CloseSocket() {
  LOG_DEBUG("Attempt to close the connection %d", sock_fd_);
  // Remove listening event
  event_del(network_event);
  event_del(workpool_event);
  // event_free(event);
  Reset();
  for (;;) {
    int status = close(sock_fd_);
    if (status < 0) {
      // failed close
      if (errno == EINTR) {
        // interrupted, try closing again
        continue;
      }
    }
    LOG_DEBUG("Already Closed the connection %d", sock_fd_);
    return Transition::NONE;
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
  traffic_cop_.Reset();
  next_response_ = 0;
  ssl_sent_ = false;
}

Transition NetworkConnection::Wait() {
  // TODO(tianyu): Maybe we don't need this state? Also, this name is terrible
  if (!UpdateEvent(EV_READ | EV_PERSIST)) {
    LOG_ERROR("Failed to update event, closing");
    return Transition::ERROR;
  }
  return Transition::PROCEED;
}

Transition NetworkConnection::Process() {
  // TODO(tianyu): further simplify this logic
  if (protocol_handler_ == nullptr) {
    // Process initial
    if (ssl_sent_) {
      // start SSL handshake
      // TODO: consider free conn_SSL_context
      conn_SSL_context = SSL_new(NetworkManager::ssl_context);
      if (SSL_set_fd(conn_SSL_context, sock_fd_) == 0) {
        LOG_ERROR("Failed to set SSL fd");
        PL_ASSERT(false);
      }
      int ssl_accept_ret;
      if ((ssl_accept_ret = SSL_accept(conn_SSL_context)) <= 0) {
        LOG_ERROR("Failed to accept (handshake) client SSL context.");
        LOG_ERROR("ssl error: %d", SSL_get_error(conn_SSL_context, ssl_accept_ret));
        // TODO: consider more about proper action
        PL_ASSERT(false);
        return Transition::ERROR;
      }
      LOG_ERROR("SSL handshake completed");
      ssl_sent_ = false;
    }

    switch (ProcessInitial()) {
      // TODO(tianyu): Should convert to use Transition in the top level method
      case ProcessResult::COMPLETE:
        return Transition::PROCEED;
      case ProcessResult::MORE_DATA_REQUIRED:
        return Transition::NEED_DATA;
      case ProcessResult::TERMINATE:
        return Transition::ERROR;
      default: // PROCESSING is impossible to happens in initial packets
        LOG_ERROR("Unexpected ProcessResult");
        return Transition::ERROR;
    }
  } else {
    ProcessResult status = protocol_handler_->Process(rbuf_, (size_t) handler->Id());
    switch (status) {
      case ProcessResult::MORE_DATA_REQUIRED:
        return Transition::NEED_DATA;
      case ProcessResult::COMPLETE:
        return Transition::PROCEED;
      case ProcessResult::PROCESSING: {
        if (event_del(network_event) == -1) {
          //TODO: There may be better way to handle this error
          LOG_ERROR("Failed to delete event");
          PL_ASSERT(false);
        }
        LOG_TRACE("ProcessResult: queueing");
        return Transition::GET_RESULT;
      }
      case ProcessResult::TERMINATE:
        return Transition::ERROR;
    }
  }

  // Should not be here
  return Transition::ERROR;
}

Transition NetworkConnection::ProcessWrite() {
  // TODO(tianyu): Should convert to use Transition in the top level method
  switch (WritePackets()) {
    case WriteState::WRITE_COMPLETE: {
      if (!UpdateEvent(EV_READ | EV_PERSIST)) {
        LOG_ERROR("Failed to update event, closing");
        return Transition::ERROR;
      }
      return Transition::PROCEED;
    }

    case WriteState::WRITE_NOT_READY:
      return Transition::NONE;

    case WriteState::WRITE_ERROR:
      LOG_ERROR("Error during write, closing connection");
      return Transition::ERROR;
  }

  // Should not be here
  return Transition::ERROR;
}

Transition NetworkConnection::GetResult() {
  // TODO(tianyu) We probably can collapse this state with some other state.
  if (event_add(network_event, nullptr) < 0) {
    LOG_ERROR("Failed to add event");
    PL_ASSERT(false);
  }
  protocol_handler_->GetResult();
  traffic_cop_.SetQueuing(false);
  return Transition::PROCEED;
}


}  // namespace network
}  // namespace peloton
