//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire.h
//
// Identification: src/include/wire/libevent_server.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/event.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "common/config.h"
#include "wire/wire.h"
#include "wire/socket_base.h"


namespace peloton {

namespace wire {

class Server {

public:

	Server(const PelotonConfiguration& config);

	inline ~Server() {
		for(auto socket_manager : socket_manager_vector_) {
			free(socket_manager);
		}
	}

	inline static void AddSocketManager(SocketManager<PktBuf>* socket_manager) {
		socket_manager_vector_.push_back(socket_manager);
	}

	static std::vector<SocketManager<PktBuf>*> socket_manager_vector_;  // To keep track of socket managers for deletion

private:
	// For logging purposes
	static void LogCallback(int severity, const char* msg);
	int port_;  // port number
	int max_connections_;  // maximum number of connections
};

std::vector<SocketManager<PktBuf>*> Server::socket_manager_vector_ = { };

}

}
