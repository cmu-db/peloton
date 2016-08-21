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

#include "common/logger.h"
#include "common/config.h"
#include "wire/wire.h"
#include "wire/socket_base.h"
#include "common/worker_thread_pool.h"

namespace peloton {

namespace wire {

class Server {

  public:
  Server();

  inline ~Server() {
    for (auto socket_manager : socket_manager_vector_) {
      delete(socket_manager);
    }
  }

	// Remove socket manager with the same fd and add the new one
  inline static void AddSocketManager(SocketManager<PktBuf>* socket_manager) {
  // If a socket manager with the same file descriptor exists, remove it first
    auto it = std::begin(socket_manager_vector_);
    while(it != std::end(socket_manager_vector_)) {
	  if((*it)->GetSocketFD() == socket_manager->GetSocketFD()) {
		  LOG_INFO("Removing socket manager on existing fd");
		  delete(*it);
		  it = socket_manager_vector_.erase(it);
	  }
	  else {
	    ++it;
	  }
	}
	socket_manager_vector_.push_back(socket_manager);
  }

  // To keep track of socket managers for deletion
  static std::vector<SocketManager<PktBuf>*> socket_manager_vector_;

  // socket manager id cntr
  static unsigned int socket_manager_id;

private:
  // For logging purposes
  static void LogCallback(int severity, const char* msg);

  // port number
  int port_;

  // maximum number of connections
  int max_connections_;

};

}

}

