//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handle.cpp
//
// Identification: src/network/connection_handle.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include <cstring>

#include "network/connection_dispatcher_task.h"
#include "network/connection_handle.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "network/protocol_handler_factory.h"

#include "settings/settings_manager.h"
#include "common/utility.h"

namespace peloton {
namespace network {

/*
 *  Here we are abusing macro expansion to allow for human readable definition
 *  of the finite state machine's transition table. We put these macros in an
 *  anonymous namespace to avoid them confusing people elsewhere.
 *
 *  The macros merely compose a large function. Unless you know what you are
 *  doing, do not modify their definitions.
 *
 *  To use these macros, follow the examples given. Or, here is a syntax chart:
 *
 *  x list ::= x \n (x list) (each item of x separated by new lines)
 *  graph ::= DEF_TRANSITION_GRAPH
 *              state list
 *            END_DEF
 *
 *  state ::= DEFINE_STATE(ConnState)
 *             transition list
 *            END_DEF
 *
 *  transition ::=
 *  ON (Transition) SET_STATE_TO (ConnState) AND_INVOKE (ConnectionHandle
 * method)
 *
 *  Note that all the symbols used must be defined in ConnState, Transition and
 *  ClientSocketWrapper, respectively.
 */
namespace {
// Underneath the hood these macro is defining the static method
// ConnectionHandle::StateMachine::Delta.
// Together they compose a nested switch statement. Running the function on any
// undefined state or transition on said state will throw a runtime error.
#define DEF_TRANSITION_GRAPH                                          \
  ConnectionHandle::StateMachine::transition_result                   \
  ConnectionHandle::StateMachine::Delta_(ConnState c, Transition t) { \
    switch (c) {
#define DEFINE_STATE(s) \
  case ConnState::s: {  \
    switch (t) {
#define END_DEF                                       \
  default:                                            \
    throw std::runtime_error("undefined transition"); \
    }                                                 \
    }
#define ON(t)         \
  case Transition::t: \
    return
#define SET_STATE_TO(s) \
  {                     \
  ConnState::s,
#define AND_INVOKE(m)                          \
  ([](ConnectionHandle & w) { return w.m(); }) \
  }                                            \
  ;
#define AND_WAIT                                        \
  ([](ConnectionHandle &) { return Transition::NONE; }) \
  }                                                     \
  ;
}

// clang-format off
DEF_TRANSITION_GRAPH
  DEFINE_STATE(READ)
    ON(WAKEUP) SET_STATE_TO(READ) AND_INVOKE(FillReadBuffer)
    ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
    ON(NEED_DATA) SET_STATE_TO(READ) AND_WAIT
    ON(FINISH) SET_STATE_TO(CLOSING) AND_INVOKE(CloseSocket)
  END_DEF

  DEFINE_STATE(PROCESS_WRITE_SSL_HANDSHAKE)
    ON(WAKEUP) SET_STATE_TO(PROCESS_WRITE_SSL_HANDSHAKE)
      AND_INVOKE(ProcessWrite_SSLHandshake)
    ON(NEED_DATA) SET_STATE_TO(PROCESS_WRITE_SSL_HANDSHAKE) AND_WAIT
    ON(FINISH) SET_STATE_TO(CLOSING) AND_INVOKE(CloseSocket)
    ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
  END_DEF

  DEFINE_STATE(PROCESS)
    ON(PROCEED) SET_STATE_TO(WRITE) AND_INVOKE(ProcessWrite)
    ON(NEED_DATA) SET_STATE_TO(READ) AND_INVOKE(FillReadBuffer)
    ON(GET_RESULT) SET_STATE_TO(GET_RESULT) AND_WAIT
    ON(FINISH) SET_STATE_TO(CLOSING) AND_INVOKE(CloseSocket)
    ON(NEED_SSL_HANDSHAKE) SET_STATE_TO(PROCESS_WRITE_SSL_HANDSHAKE)
      AND_INVOKE(ProcessWrite_SSLHandshake)
  END_DEF

  DEFINE_STATE(WRITE)
    ON(WAKEUP) SET_STATE_TO(WRITE) AND_INVOKE(ProcessWrite)
    ON(NEED_DATA) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
    ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
  END_DEF

  DEFINE_STATE(GET_RESULT)
    ON(WAKEUP) SET_STATE_TO(GET_RESULT) AND_INVOKE(GetResult)
    ON(PROCEED) SET_STATE_TO(WRITE) AND_INVOKE(ProcessWrite)
  END_DEF

END_DEF
    // clang-format on

void ConnectionHandle::StateMachine::Accept(Transition action,
                                            ConnectionHandle &connection) {
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
                                   std::shared_ptr<Buffer> rbuf,
                                   std::shared_ptr<Buffer> wbuf)
    : sock_fd_(sock_fd),
      handler_(handler),
      protocol_handler_(nullptr),
      rbuf_(std::move(rbuf)),
      wbuf_(std::move(wbuf)) {
  SetNonBlocking(sock_fd_);
  SetTCPNoDelay(sock_fd_);

  network_event = handler->RegisterEvent(
      sock_fd_, EV_READ | EV_PERSIST,
      METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
  workpool_event = handler->RegisterManualEvent(
      METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

  // TODO(Tianyu): should put the initialization else where.. check correctness
  // first.
  traffic_cop_.SetTaskCallback([](void *arg) {
    struct event *event = static_cast<struct event *>(arg);
    event_active(event, EV_WRITE, 0);
  }, workpool_event);
}

void ConnectionHandle::UpdateEventFlags(short flags) {
  // TODO(tianyu): The original network code seems to do this as an
  // optimization. I am leaving this out until we get numbers
  // handler->UpdateEvent(network_event, sock_fd_, flags,
  // METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

  if (flags == curr_event_flag_) return;

  handler_->UnregisterEvent(network_event);
  network_event = handler_->RegisterEvent(
      sock_fd_, flags, METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

  curr_event_flag_ = flags;
}

WriteState ConnectionHandle::WritePackets() {
  // iterate through all the packets
  for (; next_response_ < protocol_handler_->responses_.size();
       next_response_++) {
    auto pkt = protocol_handler_->responses_[next_response_].get();
    LOG_TRACE("To send packet with type: %c, len %lu",
              static_cast<char>(pkt->msg_type), pkt->len);
    // write is not ready during write. transit to WRITE
    auto result = BufferWriteBytesHeader(pkt);
    if (result == WriteState::NOT_READY) return result;
    result = BufferWriteBytesContent(pkt);
    if (result == WriteState::NOT_READY) return result;
  }

  // Done writing all packets. clear packets
  protocol_handler_->responses_.clear();
  next_response_ = 0;

  if (protocol_handler_->GetFlushFlag()) {
    return FlushWriteBuffer();
  }

  // we have flushed, disable force flush now
  protocol_handler_->SetFlushFlag(false);

  return WriteState::COMPLETE;
}

Transition ConnectionHandle::FillReadBuffer() {
  // This could be changed by SSL_ERROR_WANT_WRITE
  // When we reenter, we need to recover.
  UpdateEventFlags(EV_READ | EV_PERSIST);

  Transition result = Transition::NEED_DATA;
  ssize_t bytes_read = 0;
  bool done = false;

  // reset buffer if all the contents have been read
  if (rbuf_->buf_ptr == rbuf_->buf_size) rbuf_->Reset();

  // buf_ptr shouldn't overflow
  PELOTON_ASSERT(rbuf_->buf_ptr <= rbuf_->buf_size);

  /* Do we have leftover data and are we at the end of the buffer?
   * Move the data to the head of the buffer and clear out all the old data
   * Note: The assumption here is that all the packets/headers till
   *  rbuf_.buf_ptr have been fully processed
   */
  if (rbuf_->buf_ptr < rbuf_->buf_size &&
      rbuf_->buf_size == rbuf_->GetMaxSize()) {
    auto unprocessed_len = rbuf_->buf_size - rbuf_->buf_ptr;
    // Move this data to the head of rbuf_1
    std::memmove(rbuf_->GetPtr(0), rbuf_->GetPtr(rbuf_->buf_ptr),
                 unprocessed_len);
    // update pointers
    rbuf_->buf_ptr = 0;
    rbuf_->buf_size = unprocessed_len;
  }

  // return explicitly
  while (!done) {
    if (rbuf_->buf_size == rbuf_->GetMaxSize()) {
      // we have filled the whole buffer, exit loop
      done = true;
    } else {
      // try to fill the available space in the buffer
      // if the connection is a SSL connection, we use SSL_read, otherwise
      // we use general read function
      if (conn_SSL_context != nullptr) {
        ERR_clear_error();
        bytes_read = SSL_read(conn_SSL_context, rbuf_->GetPtr(rbuf_->buf_size),
                              rbuf_->GetMaxSize() - rbuf_->buf_size);
        LOG_TRACE("SSL read successfully");
        int err = SSL_get_error(conn_SSL_context, bytes_read);
        unsigned long ecode =
            (err != SSL_ERROR_NONE || bytes_read < 0) ? ERR_get_error() : 0;
        switch (err) {
          case SSL_ERROR_NONE: {
            // If successfully received, update buffer ptr and read status
            // keep reading till no data is available or the buffer becomes full
            rbuf_->buf_size += bytes_read;
            result = Transition::PROCEED;
            break;
          }

          case SSL_ERROR_ZERO_RETURN: {
            done = true;
            result = Transition::FINISH;
            break;
          }
          // The SSL packet is partially loaded to the SSL buffer only,
          // More data is required in order to decode the whole packet.
          case SSL_ERROR_WANT_READ: {
            LOG_TRACE("SSL packet partially loaded to SSL buffer");
            done = true;
            break;
          }
          // It happens when we're trying to rehandshake and we block on a write
          // during the handshake. We need to wait on the socket to be writable
          case SSL_ERROR_WANT_WRITE: {
            LOG_TRACE("Rehandshake during write, block until writable");
            UpdateEventFlags(EV_WRITE | EV_PERSIST);
            return Transition::NEED_DATA;
          }
          case SSL_ERROR_SYSCALL: {
            // if interrupted, try again
            if (errno == EINTR) {
              LOG_INFO("Error SSL Reading: EINTR");
              break;
            }
          }
          default: {
            throw NetworkProcessException("SSL read error: %d, error code: " +
                                          std::to_string(err) + " error code:" +
                                          std::to_string(ecode));
          }
        }
      } else {
        bytes_read = read(sock_fd_, rbuf_->GetPtr(rbuf_->buf_size),
                          rbuf_->GetMaxSize() - rbuf_->buf_size);
        LOG_TRACE("When filling read buffer, read %ld bytes", bytes_read);

        if (bytes_read > 0) {
          // read succeeded, update buffer size
          rbuf_->buf_size += bytes_read;
          result = Transition::PROCEED;
        } else if (bytes_read == 0) {
          return Transition::FINISH;
        } else if (bytes_read < 0) {
          // Nothing in the network pipe now
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // return whatever results we have
            done = true;
          } else if (errno == EINTR) {
            // interrupts are ok, try again
            continue;
          } else {
            // some other error occured
            LOG_ERROR("Error writing: %s", strerror(errno));
            throw NetworkProcessException("Error when filling read buffer " +
                                          std::to_string(errno));
          }
        }
      }
    }
  }
  return result;
}

WriteState ConnectionHandle::FlushWriteBuffer() {
  // This could be changed by unfinished write
  // When we reenter, we need to recover it to read
  UpdateEventFlags(EV_READ | EV_PERSIST);

  ssize_t written_bytes = 0;
  // while we still have outstanding bytes to write
  if (conn_SSL_context != nullptr) {
    while (wbuf_->buf_size > 0) {
      LOG_TRACE("SSL_write flush");
      ERR_clear_error();
      written_bytes = SSL_write(
          conn_SSL_context, &wbuf_->buf[wbuf_->buf_flush_ptr], wbuf_->buf_size);
      int err = SSL_get_error(conn_SSL_context, written_bytes);
      unsigned long ecode =
          (err != SSL_ERROR_NONE || written_bytes < 0) ? ERR_get_error() : 0;
      switch (err) {
        case SSL_ERROR_NONE: {
          wbuf_->buf_flush_ptr += written_bytes;
          wbuf_->buf_size -= written_bytes;
          break;
        }
        case SSL_ERROR_WANT_WRITE: {
          // The kernel will flush the network buffer automatically. What we
          // need to do is to call SSL_write() again when the buffer becomes
          // availble to write again(notified by Libevent).
          UpdateEventFlags(EV_WRITE | EV_PERSIST);
          LOG_TRACE("Flush write buffer, want write, not ready");
          return WriteState::NOT_READY;
        }
        case SSL_ERROR_WANT_READ: {
          // It happens when doing rehandshake with client.
          LOG_TRACE("Flush write buffer, want read, not ready");
          return WriteState::NOT_READY;
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
          throw NetworkProcessException("SSL write error");
        }
      }
    }
  } else {
    while (wbuf_->buf_size > 0) {
      written_bytes = 0;
      while (written_bytes <= 0) {
        LOG_TRACE("Normal write flush");
        written_bytes =
            write(sock_fd_, &wbuf_->buf[wbuf_->buf_flush_ptr], wbuf_->buf_size);
        // Write failed
        if (written_bytes < 0) {
          if (errno == EINTR) {
            // interrupts are ok, try again
            written_bytes = 0;
            continue;
            // Write would have blocked if the socket was
            // in blocking mode. Wait till it's readable
          } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Listen for socket being enabled for write
            UpdateEventFlags(EV_WRITE | EV_PERSIST);
            // We should go to CONN_WRITE state
            LOG_TRACE("WRITE NOT READY");
            return WriteState::NOT_READY;
          } else {
            // fatal errors
            LOG_ERROR("Error writing: %s", strerror(errno));
            throw NetworkProcessException("Fatal error during write");
          }
        }

        // weird edge case?
        if (written_bytes == 0 && wbuf_->buf_size != 0) {
          LOG_TRACE("Not all data is written");
          continue;
        }
      }

      // update book keeping
      wbuf_->buf_flush_ptr += written_bytes;
      wbuf_->buf_size -= written_bytes;
    }
  }
  // buffer is empty
  wbuf_->Reset();

  // we are ok
  return WriteState::COMPLETE;
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

// TODO (Tianyi) Make this to be protocol specific
// Writes a packet's header (type, size) into the write buffer.
// Return false when the socket is not ready for write
WriteState ConnectionHandle::BufferWriteBytesHeader(OutputPacket *pkt) {
  // If we should not write
  if (pkt->skip_header_write) {
    return WriteState::COMPLETE;
  }

  size_t len = pkt->len;
  unsigned char type = static_cast<unsigned char>(pkt->msg_type);
  int len_nb;  // length in network byte order

  // check if we have enough space in the buffer
  if (wbuf_->GetMaxSize() - wbuf_->buf_ptr < 1 + sizeof(int32_t)) {
    // buffer needs to be flushed before adding header
    auto result = FlushWriteBuffer();
    if (result == WriteState::NOT_READY) {
      // Socket is not ready for write
      return result;
    }
  }

  // assuming wbuf is now large enough to fit type and size fields in one go
  if (type != 0) {
    // type shouldn't be ignored
    wbuf_->buf[wbuf_->buf_ptr++] = type;
  }

  if (!pkt->single_type_pkt) {
    // make len include its field size as well
    len_nb = htonl(len + sizeof(int32_t));

    // append the bytes of this integer in network-byte order
    std::copy(reinterpret_cast<uchar *>(&len_nb),
              reinterpret_cast<uchar *>(&len_nb) + 4,
              std::begin(wbuf_->buf) + wbuf_->buf_ptr);
    // move the write buffer pointer and update size of the socket buffer
    wbuf_->buf_ptr += sizeof(int32_t);
  }

  wbuf_->buf_size = wbuf_->buf_ptr;

  // Header is written to socket buf. No need to write it in the future
  pkt->skip_header_write = true;
  return WriteState::COMPLETE;
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
  while (len != 0) {
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
      return WriteState::COMPLETE;
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
      if (result == WriteState::NOT_READY) {
        // need to retry or close connection
        return result;
      }
    }
  }
  return WriteState::COMPLETE;
}

Transition ConnectionHandle::CloseSocket() {
  LOG_DEBUG("Attempt to close the connection %d", sock_fd_);
  // Remove listening event
  handler_->UnregisterEvent(network_event);
  handler_->UnregisterEvent(workpool_event);

  if (conn_SSL_context != nullptr) {
    int shutdown_ret = 0;
    ERR_clear_error();
    shutdown_ret = SSL_shutdown(conn_SSL_context);
    if (shutdown_ret != 0) {
      int err = SSL_get_error(conn_SSL_context, shutdown_ret);
      if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
        LOG_TRACE("SSL shutdown is not finished yet");
        return Transition::NEED_DATA;
      } else {
        LOG_ERROR("Error shutting down ssl session, err: %d", err);
      }
    }
    SSL_free(conn_SSL_context);
    conn_SSL_context = nullptr;
  }

  peloton_close(sock_fd_);
  return Transition::NONE;

}

Transition ConnectionHandle::ProcessWrite_SSLHandshake() {
  // Flush out all the response first
  if (HasResponse()) {
    auto write_ret = ProcessWrite();
    if (write_ret != Transition::PROCEED) {
      return write_ret;
    }
  }

  return SSLHandshake();
}

Transition ConnectionHandle::SSLHandshake() {
  if (conn_SSL_context == nullptr) {
    conn_SSL_context = SSL_new(PelotonServer::ssl_context);
    if (conn_SSL_context == nullptr) {
      throw NetworkProcessException("ssl context for conn failed");
    }
    SSL_set_session_id_context(conn_SSL_context, nullptr, 0);
    if (SSL_set_fd(conn_SSL_context, sock_fd_) == 0) {
      LOG_ERROR("Failed to set SSL fd");
      return Transition::FINISH;
    }
  }

  // TODO(Yuchen): post-connection verification?
  // clear current thread's error queue before any OpenSSL call
  ERR_clear_error();
  int ssl_accept_ret = SSL_accept(conn_SSL_context);
  if (ssl_accept_ret > 0) return Transition::PROCEED;

  int err = SSL_get_error(conn_SSL_context, ssl_accept_ret);
  int ecode = ERR_get_error();
  char error_string[120];
  ERR_error_string(ecode, error_string);
  switch (err) {
    case SSL_ERROR_SSL: {
      if (ecode < 0) {
        LOG_ERROR("Could not accept SSL connection");
      } else {
        LOG_ERROR(
            "Could not accept SSL connection: EOF detected, "
            "ssl_error_ssl, %s",
            error_string);
      }
      return Transition::FINISH;
    }
    case SSL_ERROR_ZERO_RETURN: {
      LOG_ERROR(
          "Could not accept SSL connection: EOF detected, "
          "ssl_error_zero_return, %s",
          error_string);
      return Transition::FINISH;
    }
    case SSL_ERROR_SYSCALL: {
      if (ecode < 0) {
        LOG_ERROR("Could not accept SSL connection, %s", error_string);
      } else {
        LOG_ERROR(
            "Could not accept SSL connection: EOF detected, "
            "ssl_sys_call, %s",
            error_string);
      }
      return Transition::FINISH;
    }
    case SSL_ERROR_WANT_READ: {
      UpdateEventFlags(EV_READ | EV_PERSIST);
      return Transition::NEED_DATA;
    }
    case SSL_ERROR_WANT_WRITE: {
      UpdateEventFlags(EV_WRITE | EV_PERSIST);
      return Transition::NEED_DATA;
    }
    default: {
      LOG_ERROR("Unrecognized SSL error code: %d", err);
      return Transition::FINISH;
    }
  }
}

Transition ConnectionHandle::Process() {
  if (protocol_handler_ == nullptr) {
    // TODO(Tianyi) Check the rbuf here before we create one if we have
    // another protocol handler
    protocol_handler_ = ProtocolHandlerFactory::CreateProtocolHandler(
        ProtocolHandlerType::Postgres, &traffic_cop_);
  }

  ProcessResult status =
      protocol_handler_->Process(*rbuf_, (size_t)handler_->Id());

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
    case ProcessResult::NEED_SSL_HANDSHAKE:
      return Transition::NEED_SSL_HANDSHAKE;
    default:
      LOG_ERROR("Unknown process result");
      throw NetworkProcessException("Unknown process result");
  }
}

Transition ConnectionHandle::ProcessWrite() {
  // TODO(tianyu): Should convert to use Transition in the top level method
  switch (WritePackets()) {
    case WriteState::COMPLETE:
      UpdateEventFlags(EV_READ | EV_PERSIST);
      return Transition::PROCEED;
    case WriteState::NOT_READY:
      return Transition::NONE;
  }
  throw NetworkProcessException("Unexpected write state");
}

Transition ConnectionHandle::GetResult() {
  // TODO(tianyu) We probably can collapse this state with some other state.
  if (event_add(network_event, nullptr) < 0) {
    LOG_ERROR("Failed to add event");
    PELOTON_ASSERT(false);
  }
  protocol_handler_->GetResult();
  traffic_cop_.SetQueuing(false);
  return Transition::PROCEED;
}
}  // namespace network
}  // namespace peloton
