//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_manager.h
//
// Identification: src/backend/networking/connection_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/mutex.h"
#include "backend/networking/tcp_connection.h"

#include <event2/event.h>
#include <pthread.h>

namespace peloton {
namespace networking {
//===--------------------------------------------------------------------===//
// Connection Manager
// Connection Manager can be used by both server and client
//===--------------------------------------------------------------------===//

class ConnectionManager {
  ConnectionManager& operator=(const ThreadManager&) = delete;
  ConnectionManager& operator=(ThreadManager&&) = delete;

 public:
  // global singleton
  static ConnectionManager& GetInstance(void);

  void ResterRpcServer(RpcServer* server);
  RpcServer* GetRpcServer();

  struct event_base* GetEventBase();

  Connection* GetConn(std::string& addr);

  Connection* GetConn(NetworkAddress& addr);
  Connection* CreateConn(NetworkAddress& addr);
  Connection* FindConn(NetworkAddress& addr);
  bool AddConn(NetworkAddress addr, Connection* conn);
  bool AddConn(struct sockaddr& addr, Connection* conn);
  bool DeleteConn(NetworkAddress& addr);
  bool DeleteConn(Connection* conn);

  ConnectionManager();
  ~ConnectionManager();

  long start_time_;

 private:
  // rpc server
  RpcServer* rpc_server_;

  // all the connections established: addr --> connection instance
  std::map<NetworkAddress, Connection*> conn_pool_;

  // a connection can be shared among pthreads
  Mutex mutex_;
  Condition cond_;

  //////////////////////////////////////////////
  // The following is only for performance test
  //////////////////////////////////////////////
  std::map<NetworkAddress, Connection*> client_conn_pool_;
};

}  // End peloton networking
}  // End peloton namespace
