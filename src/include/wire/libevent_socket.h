//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// socket_base.h
//
// Identification: src/include/wire/socket_base.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <vector>
#include <thread>
#include <cstring>
#include <mutex>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <iostream>
#include <event2/event.h>
#include <chrono>

#include "common/logger.h"
#include "common/config.h"
#include "wire/libevent_server.h"

#define SOCKET_BUFFER_SIZE 8192

using namespace std::chrono;

namespace peloton {
namespace wire {

class PacketManager;
class Server;

typedef unsigned char uchar;

// use array as memory for the socket buffers can be fixed
typedef std::array<uchar, SOCKET_BUFFER_SIZE> SockBuf;


// Buffers used to batch messages at the socket
struct Buffer {
  size_t buf_ptr;   // buffer cursor
  size_t buf_size;  // buffer size
  SockBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0) {}

  inline void Reset() {
    buf_ptr = 0;
    buf_size = 0;
  }

  inline size_t GetMaxSize() { return SOCKET_BUFFER_SIZE; }
};
/*
 * SocketManager - Wrapper for managing socket.
 * 	B is the STL container type used as the protocol's buffer.
 */
template <typename B>
class LibeventSocket {
 public:
	int sock_fd;  // socket file descriptor
	bool is_disconnected; // is the connection disconnected
	struct event *event; // libevent handle
	short event_flags;  // event flags mask
	Buffer rbuf;  // Socket's read buffer
	Buffer wbuf;  // Socket's write buffer
	std::shared_ptr<LibeventThread> thread; // reference to the libevent thread
	std::unique_ptr<PacketManager> pkt_manager; // Stores state for this socket

private:
	/* refill_read_buffer - Used to repopulate read buffer with a fresh
	 * batch of data from the socket
	 */
	bool RefillReadBuffer();

	inline void Init(short event_flags, std::shared_ptr<LibeventThread>& thread) {
		is_disconnected = false;
		this->event_flags = event_flags;
		this->thread = thread;
		event = event_new(thread->libevent_base_, sock_fd, event_flags, EventHandler, nullptr);
		event_add(event, nullptr);
	}

 public:
	inline LibeventSocket(int sock_fd, short event_flags,
												std::shared_ptr<LibeventThread>& thread) :
			sock_fd(sock_fd), event_flags(event_flags), thread(thread) {
		Init(event_flags, thread);
	}


	// Reads a packet of length "bytes" from the head of the buffer
	bool ReadBytes(B &pkt_buf, size_t bytes);

	// Writes a packet into the write buffer
	bool BufferWriteBytes(B &pkt_buf, size_t len, uchar type);

	void PrintWriteBuffer();

	// Used to invoke a write into the Socket, once the write buffer is ready
	bool FlushWriteBuffer();

	void CloseSocket();

	/* Resuse this object for a new connection. We could be assigned to a
	 * new thread, change thread reference.
	 */
	void Reset(short event_flags, std::shared_ptr<LibeventThread>& thread) {
		is_disconnected = false;
		rbuf.Reset();
		wbuf.Reset();
		pkt_manager.reset(nullptr);
		Init(event_flags, thread);
	}
};

// Thread function created per client
template <typename P, typename B>
void ClientHandler(std::unique_ptr<int> clientfd);

// Server's "accept loop"
template <typename P, typename B>
void HandleConnections(Server *server);

}
}
