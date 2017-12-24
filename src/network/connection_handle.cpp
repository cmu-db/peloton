//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle.cpp
//
// Identification: src/include/network/connection_handle.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include "network/connection_handle.h"
#include <include/network/postgres_protocol_handler.h>
#include "network/protocol_handler_factory.h"
#include "network/connection_dispatcher_task.h"
#include "network/peloton_server.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace peloton {
namespace network {

/*
 *  Here we are abusing macro expansion to allow for human readable definition of the
 *  finite state machine's transition table. We put these macros in an anonymous
 *  namespace to avoid them confusing people elsewhere.
 *
 *  The macros merely compose a large function. Unless you know what you are doing, do not modify their
 *  definitions.
 *
 *  To use these macros, follow the examples given. Or, here is a syntax chart:
 *
 *  x list ::= x... (separated by new lines)
 *  graph ::= DEF_TRANSITION_GRAPH
 *              state_list
 *            END_DEF

 *  state ::= DEFINE_STATE(ConnState)
 *             transition_list
 *            END_DEF
 *
 *  transition ::= ON (Transition) SET_STATE_TO (ConnState) AND_INVOKE (ClientSocketWrapper method)
 *
 *  Note that all the symbols used must be defined in ConnState, Transition and ClientSocketWrapper, respectively.
 */
namespace {
  // Underneath the hood these macro is defining the static method ConncetionHandle::StateMachine::Delta
  // Together they compose a nested switch statement. Running the function on any undefined
  // state or transition on said state will throw a runtime error.
  #define DEF_TRANSITION_GRAPH \
        ConnectionHandle::StateMachine::transition_result ConnectionHandle::StateMachine::Delta_(ConnState c, Transition t) { \
          switch (c) {
  #define DEFINE_STATE(s) case ConnState::s: { switch (t) {
  #define END_DEF default: throw std::runtime_error("undefined transition"); }}
  #define ON(t) case Transition::t: return
  #define SET_STATE_TO(s) {ConnState::s,
  #define AND_INVOKE(m) ([] (ConnectionHandle &w) { return w.m(); })};
  #define AND_WAIT ([] (ConnectionHandle &){ return Transition::NONE; })};
}

DEF_TRANSITION_GRAPH
  DEFINE_STATE (READ)
    ON (WAKEUP) SET_STATE_TO (READ) AND_INVOKE (FillReadBuffer)
    ON (PROCEED) SET_STATE_TO (PROCESS) AND_INVOKE (Process)
    ON (NEED_DATA) SET_STATE_TO (WAIT) AND_INVOKE (Wait)
    ON (FINISH) SET_STATE_TO (CLOSING) AND_INVOKE (CloseSocket)
  END_DEF

  DEFINE_STATE (WAIT)
    ON (PROCEED) SET_STATE_TO (READ) AND_WAIT
  END_DEF

  DEFINE_STATE (PROCESS)
    ON (PROCEED) SET_STATE_TO (WRITE) AND_INVOKE (ProcessWrite)
    ON (NEED_DATA) SET_STATE_TO (WAIT) AND_INVOKE (Wait)
    ON (GET_RESULT) SET_STATE_TO (GET_RESULT) AND_WAIT
    ON (FINISH) SET_STATE_TO (CLOSING) AND_INVOKE (CloseSocket)
  END_DEF

  DEFINE_STATE (WRITE)
    ON (WAKEUP) SET_STATE_TO (WRITE) AND_INVOKE (ProcessWrite)
    ON (PROCEED) SET_STATE_TO (PROCESS) AND_INVOKE (Process)
  END_DEF

  DEFINE_STATE (GET_RESULT)
    ON (WAKEUP) SET_STATE_TO (GET_RESULT) AND_INVOKE (GetResult)
    ON (PROCEED) SET_STATE_TO (WRITE) AND_INVOKE (ProcessWrite)
  END_DEF
END_DEF

void ConnectionHandle::StateMachine::Accept(Transition action, ConnectionHandle &connection)   {
  Transition next = action;
  while (next != Transition::NONE) {
    transition_result result = Delta_(current_state_, next);
    current_state_ = result.first;
    try {
      next = result.second(connection);
    } catch (NetworkProcessException &e) {
      LOG_ERROR("%s\n", e.what());
      connection.CloseSocket();
      return;
    }
  }
}

ConnectionHandle::ConnectionHandle(int sock_fd, ConnectionHandlerTask *handler,
                                   std::shared_ptr<Buffer> rbuf, std::shared_ptr<Buffer> wbuf)
    : sock_fd_(sock_fd),
      handler_(handler),
      protocol_handler_(nullptr),
      rbuf_(std::move(rbuf)),
      wbuf_(std::move(wbuf)){
  SetNonBlocking(sock_fd_);
  SetTCPNoDelay(sock_fd_);

//  TODO(tianyu): The original network code seems to do this as an optimization. I am leaving this out until we get numbers
//  if (network_event != nullptr)
//    handler->UpdateEvent(network_event, sock_fd_, event_flags,
//                         METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
//  else
//    network_event = handler->RegisterEvent(sock_fd_, event_flags,
//                                           METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
//
//  if (workpool_event != nullptr)
//    handler->UpdateManualEvent(workpool_event, METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
//  else
//    workpool_event = handler->RegisterManualEvent(METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

  network_event = handler->RegisterEvent(sock_fd_, EV_READ|EV_PERSIST,
                                         METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
  workpool_event = handler->RegisterManualEvent(METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

  //TODO: should put the initialization else where.. check correctness first.
  traffic_cop_.SetTaskCallback([](void *arg) {
    struct event* event = static_cast<struct event*>(arg);
    event_active(event, EV_WRITE, 0);
  }, workpool_event);

}

void ConnectionHandle::UpdateEventFlags(short flags) {
  // TODO(tianyu): The original network code seems to do this as an optimization. I am leaving this out until we get numbers
  // handler->UpdateEvent(network_event, sock_fd_, flags, METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
  handler_->UnregisterEvent(network_event);
  network_event = handler_->RegisterEvent(sock_fd_, flags, METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
}

WriteState ConnectionHandle::WritePackets() {
  // iterate through all the packets
  for (; next_response_ < protocol_handler_->responses.size(); next_response_++) {
    auto pkt = protocol_handler_->responses[next_response_].get();
    LOG_TRACE("To send packet with type: %c", static_cast<char>(pkt->msg_type));
    // write is not ready during write. transit to WRITE
    auto result = BufferWriteBytesHeader(pkt);
    if (result == WriteState::WRITE_NOT_READY) return result;
    result = BufferWriteBytesContent(pkt);
    if (result == WriteState::WRITE_NOT_READY) return result;
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

Transition ConnectionHandle::FillReadBuffer() {
  Transition result = Transition::NEED_DATA;
  ssize_t bytes_read = 0;
  bool done = false;

  // reset buffer if all the contents have been read
  if (rbuf_->buf_ptr == rbuf_->buf_size) rbuf_->Reset();

  // buf_ptr shouldn't overflow
  PL_ASSERT(rbuf_->buf_ptr <= rbuf_->buf_size);

  /* Do we have leftover data and are we at the end of the buffer?
   * Move the data to the head of the buffer and clear out all the old data
   * Note: The assumption here is that all the packets/headers till
   *  rbuf_->buf_ptr have been fully processed
   */
  if (rbuf_->buf_ptr < rbuf_->buf_size && rbuf_->buf_size == rbuf_->GetMaxSize()) {
    auto unprocessed_len = rbuf_->buf_size - rbuf_->buf_ptr;
    // Move this data to the head of rbuf_1
    std::memmove(rbuf_->GetPtr(0), rbuf_->GetPtr(rbuf_->buf_ptr), unprocessed_len);
    // update pointers
    rbuf_->buf_ptr = 0;
    rbuf_->buf_size = unprocessed_len;
  }

  // return explicitly
  while (done == false) {
    if (rbuf_->buf_size == rbuf_->GetMaxSize()) {
      // we have filled the whole buffer, exit loop
      done = true;
    } else {
      // try to fill the available space in the buffer
      // if the connection is a SSL connection, we use SSL_read, otherwise
      // we use general read function
      if (conn_SSL_context != nullptr) {
        bytes_read = SSL_read(conn_SSL_context, rbuf_->GetPtr(rbuf_->buf_size),
                              rbuf_->GetMaxSize() - rbuf_->buf_size);
      }
      else {
        bytes_read = read(sock_fd_, rbuf_->GetPtr(rbuf_->buf_size),
                          rbuf_->GetMaxSize() - rbuf_->buf_size);
        LOG_TRACE("When filling read buffer, read %ld bytes", bytes_read);
      }

      if (bytes_read > 0) {
        // read succeeded, update buffer size
        rbuf_->buf_size += bytes_read;
        result = Transition::PROCEED;
      } else if (bytes_read == 0) {
        return Transition::FINISH;
      } else if (bytes_read < 0) {
        ErrorUtil::LogErrno();
        // related to non-blocking?
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // return whatever results we have
          done = true;
        } else if (errno == EINTR) {
          // interrupts are ok, try again
          continue;
        } else {
          // some other error occured
          throw NetworkProcessException("Error when filling read buffer " + std::to_string(errno));
        }
      }
    }
  }
  return result;
}

WriteState ConnectionHandle::FlushWriteBuffer() {
  ssize_t written_bytes = 0;
  // while we still have outstanding bytes to write
  while (wbuf_->buf_size > 0) {
    written_bytes = 0;
    while (written_bytes <= 0) {
      if (conn_SSL_context != nullptr) {
        written_bytes =
            SSL_write(conn_SSL_context, &wbuf_->buf[wbuf_->buf_flush_ptr], wbuf_->buf_size);
      }
      else {
        written_bytes =
            write(sock_fd_, &wbuf_->buf[wbuf_->buf_flush_ptr], wbuf_->buf_size);
      }
      // Write failed
      if (written_bytes < 0) {
        ErrorUtil::LogErrno();
        if (errno == EINTR) {
          // interrupts are ok, try again
          written_bytes = 0;
          continue;
          // Write would have blocked if the socket was
          // in blocking mode. Wait till it's readable
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // Listen for socket being enabled for write
          UpdateEventFlags(EV_WRITE | EV_PERSIST);
          // We should go to WRITE state
          LOG_DEBUG("WRITE NOT READY");
          return WriteState::WRITE_NOT_READY;
        } else {
          // fatal errors
          throw NetworkProcessException("Fatal error during write, errno " + std::to_string(errno));
        }
      }

      // weird edge case?
      if (written_bytes == 0 && wbuf_->buf_size != 0) {
        LOG_DEBUG("Not all data is written");
        continue;
      }
    }

    // update book keeping
    wbuf_->buf_flush_ptr += written_bytes;
    wbuf_->buf_size -= written_bytes;
  }

  // buffer is empty
  wbuf_->Reset();

  // we are ok
  return WriteState::WRITE_COMPLETE;
}

std::string ConnectionHandle::WriteBufferToString() {
#ifdef LOG_TRACE_ENABLED
  LOG_TRACE("Write Buffer:");

for (size_t i = 0; i < wbuf_->buf_size; ++i) {
  LOG_TRACE("%u", wbuf_->buf[i]);
}
#endif

  return std::string(wbuf_->buf.begin(), wbuf_->buf.end());
}

ProcessResult ConnectionHandle::ProcessInitial() {
  //TODO: this is a direct copy and we should get rid of it later;
  InputPacket rpkt;

  if (rpkt.header_parsed == false) {
    // parse out the header first
    if (ReadStartupPacketHeader(*rbuf_, rpkt) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }
  PL_ASSERT(rpkt.header_parsed == true);

  if (rpkt.is_initialized == false) {
    // packet needs to be initialized with rest of the contents
    //TODO: If other protocols are added, this need to be changed
    if (PostgresProtocolHandler::ReadPacket(*rbuf_, rpkt) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }

  // We need to handle startup packet first
  ProcessInitialPacket(&rpkt);
  return ProcessResult::COMPLETE;
}

// TODO: This function is now dedicated for postgres packet
bool ConnectionHandle::ReadStartupPacketHeader(Buffer &rbuf, InputPacket &rpkt) {
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

void ConnectionHandle::ProcessInitialPacket(InputPacket *pkt) {
  std::string token, value;
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  int32_t proto_version = PacketGetInt(pkt, sizeof(int32_t));
  LOG_INFO("protocol version: %d", proto_version);

  // TODO: consider more about return value
  if (proto_version == SSL_MESSAGE_VERNO) {
    LOG_TRACE("process SSL MESSAGE");
    ProcessSSLRequestPacket(pkt);
  }
  else {
    LOG_TRACE("process startup packet");
    ProcessStartupPacket(pkt, proto_version);
  }
}

void ConnectionHandle::ProcessSSLRequestPacket(InputPacket *) {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  // TODO: consider more about a proper response
  response->msg_type = NetworkMessageType::SSL_YES;
  protocol_handler_->responses.push_back(std::move(response));
  protocol_handler_->SetFlushFlag(true);
  ssl_sent_ = true;
}

void ConnectionHandle::ProcessStartupPacket(InputPacket *pkt, int32_t proto_version) {
  std::string token, value;

  // Only protocol version 3 is supported
  if (PROTO_MAJOR_VERSION(proto_version) != 3)
    throw NetworkProcessException("Protocol error: Only protocol version 3 is supported.");

  // TODO: check for more malformed cases
  // iterate till the end
  // TODO(tianyu): WTF is this?
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
}

// Writes a packet's header (type, size) into the write buffer.
// Return false when the socket is not ready for write
WriteState ConnectionHandle::BufferWriteBytesHeader(OutputPacket *pkt) {
  // If we should not write
  if (pkt->skip_header_write) {
    return WriteState::WRITE_COMPLETE;
  }

  size_t len = pkt->len;
  unsigned char type = static_cast<unsigned char>(pkt->msg_type);
  int len_nb;  // length in network byte order

  // check if we have enough space in the buffer
  if (wbuf_->GetMaxSize() - wbuf_->buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    auto result = FlushWriteBuffer();
    if (result == WriteState::WRITE_NOT_READY) {
      // Socket is not ready for write
      return result;
    }
  }

  // assuming wbuf is now large enough to fit type and size fields in one go
  if (type != 0) {
    // type shouldn't be ignored
    wbuf_->buf[wbuf_->buf_ptr++] = type;
  }

  // make len include its field size as well
  len_nb = htonl(len + sizeof(int32_t));

  // append the bytes of this integer in network-byte order
  std::copy(reinterpret_cast<uchar *>(&len_nb),
            reinterpret_cast<uchar *>(&len_nb) + 4,
            std::begin(wbuf_->buf) + wbuf_->buf_ptr);

  // move the write buffer pointer and update size of the socket buffer
  wbuf_->buf_ptr += sizeof(int32_t);
  wbuf_->buf_size = wbuf_->buf_ptr;

  // Header is written to socket buf. No need to write it in the future
  pkt->skip_header_write = true;
  return WriteState::WRITE_COMPLETE;
}

// Writes a packet's content into the write buffer
// Return false when the socket is not ready for write
WriteState ConnectionHandle::BufferWriteBytesContent(OutputPacket *pkt) {
  // the packet content to write
  ByteBuf &pkt_buf = pkt->buf;
  // the length of remaining content to write
  size_t len = pkt->len;
  // window is the size of remaining space in socket's wbuf
  size_t window = 0;

  // fill the contents
  while (len) {
    // calculate the remaining space in wbuf
    window = wbuf_->GetMaxSize() - wbuf_->buf_ptr;
    if (len <= window) {
      // contents fit in the window, range copy "len" bytes
      std::copy(std::begin(pkt_buf) + pkt->write_ptr,
                std::begin(pkt_buf) + pkt->write_ptr + len,
                std::begin(wbuf_->buf) + wbuf_->buf_ptr);

      // Move the cursor and update size of socket buffer
      wbuf_->buf_ptr += len;
      wbuf_->buf_size = wbuf_->buf_ptr;
      LOG_TRACE("Content fit in window. Write content successful");
      return WriteState::WRITE_COMPLETE;
    } else {
      // contents longer than socket buffer size, fill up the socket buffer
      // with "window" bytes

      std::copy(std::begin(pkt_buf) + pkt->write_ptr,
                std::begin(pkt_buf) + pkt->write_ptr + window,
                std::begin(wbuf_->buf) + wbuf_->buf_ptr);

      // move the packet's cursor
      pkt->write_ptr += window;
      len -= window;
      // Now the wbuf is full
      wbuf_->buf_size = wbuf_->GetMaxSize();

      LOG_TRACE("Content doesn't fit in window. Try flushing");
      auto result = FlushWriteBuffer();
      // flush before write the remaining content
      if (result == WriteState::WRITE_NOT_READY) {
        // need to retry or close connection
        return result;
      }
    }
  }
  return WriteState::WRITE_COMPLETE;
}

Transition ConnectionHandle::CloseSocket() {
  LOG_DEBUG("Attempt to close the connection %d", sock_fd_);
  // Remove listening event
  handler_->UnregisterEvent(network_event);
  handler_->UnregisterEvent(workpool_event);
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

Transition ConnectionHandle::Wait() {
  // TODO(tianyu): Maybe we don't need this state? Also, this name is terrible
  UpdateEventFlags(EV_READ | EV_PERSIST);
  return Transition::PROCEED;
}

Transition ConnectionHandle::Process() {
  // TODO(tianyu): further simplify this logic
  if (protocol_handler_ == nullptr) {
    // Process initial
    if (ssl_sent_) {
      // start SSL handshake
      // TODO: consider free conn_SSL_context
      conn_SSL_context = SSL_new(PelotonServer::ssl_context);
      if (SSL_set_fd(conn_SSL_context, sock_fd_) == 0) {
        LOG_ERROR("Failed to set SSL fd");
        PL_ASSERT(false);
      }
      int ssl_accept_ret;
      if ((ssl_accept_ret = SSL_accept(conn_SSL_context)) <= 0) {
        LOG_ERROR("Failed to accept (handshake) client SSL context.");
        throw NetworkProcessException("ssl error: " + std::to_string(SSL_get_error(conn_SSL_context, ssl_accept_ret)));
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
      default:
        throw NetworkProcessException("Unexpected Process Initial result");
    }
  } else {
    ProcessResult status = protocol_handler_->Process(*rbuf_, (size_t) handler_->Id());

    switch (status) {
      case ProcessResult::MORE_DATA_REQUIRED:
        return Transition::NEED_DATA;
      case ProcessResult::COMPLETE:
        return Transition::PROCEED;
      case ProcessResult::PROCESSING:
        EventUtil::EventDel(network_event);
        LOG_TRACE("ProcessResult: queueing");
        return Transition::GET_RESULT;
      case ProcessResult::TERMINATE:
        return Transition::FINISH;
    }
  }
  throw NetworkProcessException("Unexpected process result");
}

Transition ConnectionHandle::ProcessWrite() {
  // TODO(tianyu): Should convert to use Transition in the top level method
  switch (WritePackets()) {
    case WriteState::WRITE_COMPLETE:
      UpdateEventFlags(EV_READ | EV_PERSIST);
      return Transition::PROCEED;
    case WriteState::WRITE_NOT_READY:
      return Transition::NONE;
  }

}

Transition ConnectionHandle::GetResult() {
  // TODO(tianyu) We probably can collapse this state with some other state.
  if (event_add(network_event, nullptr) < 0) {
    LOG_ERROR("Failed to add event");
    PL_ASSERT(false);
  }
  protocol_handler_->GetResult();
  traffic_cop_.SetQueuing(false);
  return Transition::PROCEED;
}

}
}
