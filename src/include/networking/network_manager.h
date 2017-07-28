//
// Created by tim on 25/07/17.
//
#include <vector>

#include "networking/network_thread.h"
#include "networking/protocol_handler.h"
#include "networking/packet_manager.h"
#pragma once

namespace peloton {
namespace networking {
// Buffers used to batch messages at the socket
struct Buffer {
  size_t buf_ptr;        // buffer cursor
  size_t buf_size;       // buffer size
  size_t buf_flush_ptr;  // buffer cursor for write
  ByteBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0), buf_flush_ptr(0) {
    // capacity of the buffer
    buf.reserve(SOCKET_BUFFER_SIZE);
  }

  inline void Reset() {
    buf_ptr = 0;
    buf_size = 0;
    buf_flush_ptr = 0;
  }

  // single buffer element accessor
  inline uchar GetByte(size_t &index) { return buf[index]; }

  // Get pointer to index location
  inline uchar *GetPtr(size_t index) { return &buf[index]; }

  inline ByteBuf::const_iterator Begin() { return std::begin(buf); }

  inline ByteBuf::const_iterator End() { return std::end(buf); }

  inline size_t GetMaxSize() { return buf.size(); }
};
/*
 * NetworkManager - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
class NetworkManager {
 public:
  int thread_id;
  int sock_fd;                    // socket file descriptor
  struct event *event = nullptr;  // libevent handle
  short event_flags;              // event flags mask

  SSL* conn_SSL_context = nullptr;          // SSL context for the connection

  NetworkThread *thread;          // reference to the libevent thread
  ProtocolHandler protocol_handler_;       // Stores state for this socket
  std::unique_ptr<PacketManager> packet_manager_;
  ConnState state = CONN_INVALID;  // Initial state of connection
  std::queue<std::unique_ptr<InputPacket>> rpkts_;                // Used for reading a single Postgres packet

  Client client_;
 private:
  Buffer rbuf_;                     // Socket's read buffer
  Buffer wbuf_;                     // Socket's write buffer
  unsigned int next_response_ = 0;  // The next response in the response buffer

 private:
  // Is the requested amount of data available from the current position in
  // the reader buffer?
  bool IsReadDataAvailable(size_t bytes);

  // Parses out packet size from its header
  void GetSizeFromPktHeader(size_t start_index);

 public:
  inline NetworkManager(int sock_fd, short event_flags, NetworkThread *thread,
                        ConnState init_state)
      : sock_fd(sock_fd) {
    Init(event_flags, thread, init_state);
  }
  /** Get the initial packet from buffer initialize protocol handler
   *
   * @return true if we find the initial packet and
   */
  bool GetInitialPacketFromBuffer();

  /* Routine to deal with the first packet from the client */
  int ProcessInitialPacket();

  /* Routine to deal with general Startup message */
  bool ProcessStartupPacket(std::string& contents, int32_t proto_version);

  /* Routine to deal with SSL request message */
  bool ProcessSSLRequestPacket();

  /* Reuse this object for a new connection. We could be assigned to a
   * new thread, change thread reference.
   */
  void Init(short event_flags, NetworkThread *thread, ConnState init_state);

  /* refill_read_buffer - Used to repopulate read buffer with a fresh
   * batch of data from the socket
   */
  ReadState FillReadBuffer();

  // Transit to the target state
  void TransitState(ConnState next_state);

  // Update the existing event to listen to the passed flags
  bool UpdateEvent(short flags);

  WriteState WritePackets();

  void PrintWriteBuffer();

  void CloseSocket();

  void Reset();

 private:
  // Writes a packet's header (type, size) into the write buffer
  WriteState BufferWriteBytesHeader(OutputPacket *pkt);

  // Writes a packet's content into the write buffer
  WriteState BufferWriteBytesContent(OutputPacket *pkt);

  // Used to invoke a write into the Socket, returns false if the socket is not
  // ready for write
  WriteState FlushWriteBuffer();
};

}
}