//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// socket_base.cpp
//
// Identification: src/wire/libevent_worker.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "libevent_server.h"

namespace peloton {

namespace wire {

void WorkerHandleNewConn(evutil_socket_t local_fd, short ev_flags, void *arg) {
	char m_buf[1]; // buffer used to receive messages from the main thread
	std::shared_ptr<NewConnQueueItem> item;
	LibeventSocket *conn;
	std::shared_ptr<LibeventThread> thread = static_cast<std::shared_ptr<LibeventThread>>(arg);

	// read the operation that needs to be performed
	if (read(local_fd, m_buf, 1)) {
		LOG_ERROR("Can't read from the libevent pipe");
		return;
	}

	switch (m_buf[0]) {
		/* new connection case */
		case 'n': {
			// fetch the new connection fd from the queue
			thread->new_conn_queue.Dequeue(item);
			conn = LibeventServer::GetConn(item->new_conn_fd);
			if (conn == nullptr) {
				/* create a new connection object */
				LibeventServer::CreateNewConn(item->new_conn_fd, item->event_flags,
																			thread);
			} else {
				/* otherwise reset and reuse the existing conn object */
				conn->Reset(item->event_flags, thread);
			}
			break;
		}

		default :
			LOG_ERROR("Shouldn't reach here");
	}
}

void EventHandler(evutil_socket_t connfd,
									UNUSED_ATTRIBUTE short ev_flags,
									UNUSED_ATTRIBUTE void *arg) {
	LOG_ERROR("Event callback fired for connfd:%d", connfd);
}

}

}
}
