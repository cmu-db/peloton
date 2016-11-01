//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_callbacks.cpp
//
// Implements Libevent callbacks for the protocol and their helpers
//
// Identification: src/wire/libevent_callbacks.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include "wire/libevent_server.h"
#include "common/macros.h"

namespace peloton {
namespace wire {

void WorkerHandleNewConn(evutil_socket_t local_fd, UNUSED_ATTRIBUTE short ev_flags, void *arg) {
	char m_buf[1]; // buffer used to receive messages from the main thread
	std::shared_ptr<NewConnQueueItem> item;
	LibeventSocket *conn;
	LibeventWorkerThread *thread = static_cast<LibeventWorkerThread *>(arg);

	// read the operation that needs to be performed
	if (read(local_fd, m_buf, 1)) {
		LOG_ERROR("Can't read from the libevent pipe");
		return;
	}

	switch (m_buf[0]) {
		/* new connection case */
		case 'c': {
			// fetch the new connection fd from the queue
			thread->new_conn_queue.Dequeue(item);
			conn = LibeventServer::GetConn(item->new_conn_fd);
			if (conn == nullptr) {
				/* create a new connection object */
				LibeventServer::CreateNewConn(item->new_conn_fd, item->event_flags,
																			static_cast<LibeventThread*>(thread));
			} else {
				/* otherwise reset and reuse the existing conn object */
				conn->Reset(item->event_flags, static_cast<LibeventThread*>(thread));
			}
			break;
		}

		default :
			LOG_ERROR("Shouldn't reach here");
	}
}

void EventHandler(evutil_socket_t connfd, short ev_flags, void *arg) {
	LOG_ERROR("Event callback fired for connfd:%d", connfd);
	LibeventSocket *conn = static_cast<LibeventSocket *>(arg);
	PL_ASSERT(conn != nullptr);
	conn->event_flags = ev_flags;
	PL_ASSERT(connfd == conn->sock_fd);
	StateMachine(conn);
}


void StateMachine(LibeventSocket *conn) {
	// LOG_ERROR("Handler invoked");
	bool done = false;
	int state = 1;
	// Assume 1 is for LISTEN_STATE..
	// TODO Define all different states as readable variables in header

	while (done == false) {
		switch (state) {
			case 1: {
				struct sockaddr_storage addr;
				socklen_t addrlen = sizeof(addr);
				int new_conn_fd = accept(conn->sock_fd, (struct sockaddr *)&addr, &addrlen);
				if (new_conn_fd == -1) {
					LOG_ERROR("Failed to accept");
				}
				(static_cast<LibeventMasterThread *>(conn->thread))->DispatchConnection(
						new_conn_fd, EV_READ | EV_PERSIST);
				done = true;
				break;
			}
				// TODO handle other states, too
			default: {}
		}
	}
}




}
}
